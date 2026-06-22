# Realtime / Meeting Channel — Foundation Spec

## Context

Kortix agents can already act in **Slack** (a Channel), on a **schedule/webhook**
(Triggers), and through **Connectors**. They cannot yet *join a live meeting*.
We want a native **Realtime Channel**: an agent that joins a Google Meet (then
Zoom/Teams), listens with speaker attribution, optionally speaks, and acts through
connectors — all inside the normal session/sandbox/CR model.

Two hard constraints shape the design:

1. **One interface, two implementations.** Telephony Voice (1:1) and Meet (N-party)
   are both `RealtimeChannel`. Build the interface so both fit; ship Meet first.
2. **The agent must not know it's "on a call."** The Channel is a pure translator:
   inbound speaker turns → ordinary session messages tagged with *who spoke*; the
   session's text replies → spoken audio out. The single new concept the session
   learns is **silence is the default; speaking is the exception.**

We build our **own thin transport brick** on an OSS stack (Pipecat + faster-whisper
+ Kokoro/Chatterbox; LLM from Kortix). We are **not** adopting Vexa as a platform —
we **lift specific `vexa-bot` capture/join internals** (Apache-2.0) and rebuild the
rest. Cited files in §3.

This slots into the platform exactly where Slack does — verified against the live
code, not assumed:

- **Inbound** channels are per-platform modules feeding the *generic* session engine
  `createSession({ source, postCreate: bind_chat_thread platform, … })`
  (`apps/api/src/projects/session-lifecycle/engine.ts`). `bind_chat_thread.platform`
  is already a free `string`; chat tables are keyed `platform varchar(32)` — **no DB
  migration**. Telegram (`apps/api/src/channels/telegram-webhook.ts`, ~150 lines) is
  the cleanest reference; Slack (`channels/slack/{routes,dispatch,session,turn}.ts`)
  is the richest.
- **Outbound** is the generic `channel`-connector half: a sandbox CLI auto-shimmed
  from `apps/sandbox/slack-cli/channels/*.ts` (drop `meet.ts`, `install-shims.sh`
  generates the `meet` binary — no Dockerfile edit), talking to the Executor gateway
  (`apps/api/src/executor/{gateway,channels,channel-materialize,db-deps}.ts`), which
  resolves credentials **server-side**. Allow-list lives in
  `apps/api/src/projects/connectors.ts` (`CHANNEL_PLATFORMS`).
- The **`/v1/projects/:projectId/turn-stream`** relay (`projects/routes/r4.ts`) +
  `chatTurnStreams` table is the generic "stream progress back" shape we reuse.

---

## 1. The `RealtimeChannel` interface

A transport-agnostic contract over a *live, bidirectional, multi-party audio session*.
Meet and Voice both implement it. It is deliberately **event-driven inbound /
method-driven outbound**, and it draws the speak-vs-silent decision as an explicit
seam the *session* owns — never the channel.

```ts
// packages/realtime-channel/src/types.ts  (proposed)

type ParticipantId = string;            // stable within one meeting
interface Participant { id: ParticipantId; displayName: string; isHost?: boolean; isBot?: boolean }

interface SpeechTurn {
  speaker: Participant;                  // WHO — from diarization + DOM attribution
  text: string;                          // STT transcript for this turn
  startMs: number; endMs: number;        // relative to join (monotonic)
  isFinal: boolean;                      // interim vs settled (barge-in handling)
  confidence: number;
}

interface MediaFrame { kind: 'screen' | 'video'; participant?: ParticipantId; jpeg: Uint8Array; ts: number }

interface JoinContext {
  meetingUrl: string;
  displayName: string;                   // what others see — carries the disclosure (§5)
  platform: 'meet' | 'zoom' | 'teams' | 'voice';
  locale?: string;
}

// Events the channel EMITS (the session is the consumer)
interface RealtimeChannelEvents {
  admissionPending(): void;              // in the waiting room / "Ask to join" sent
  admissionDenied(reason: string): void;
  joined(self: Participant): void;
  participantJoined(p: Participant): void;
  participantLeft(p: ParticipantId): void;
  speechTurn(turn: SpeechTurn): void;    // ← the core inbound event
  mediaFrame(frame: MediaFrame): void;   // optional; screen-share / active video
  removed(reason: string): void;         // host kicked the bot
  ended(reason: 'host_ended' | 'empty' | 'left' | 'error'): void;
}

// Methods the channel EXPOSES (the session is the caller)
interface RealtimeChannel {
  join(ctx: JoinContext): Promise<void>;
  leave(reason?: string): Promise<void>;

  speak(text: string, opts?: { interruptible?: boolean; voice?: string }): Promise<void>;
  stopSpeaking(): Promise<void>;         // barge-in: cut TTS if a human starts talking

  participants(): Participant[];
  setMuted(muted: boolean): Promise<void>;   // listening-only posture vs ready-to-speak
  on<E extends keyof RealtimeChannelEvents>(e: E, cb: RealtimeChannelEvents[E]): void;
}
```

### The speak-vs-stay-silent decision point

This is **not** a channel method — it's a policy the session runs on every inbound
`speechTurn`, sitting in an in-sandbox **turn router** *between* the channel and the
LLM session. It exists because waking the LLM on every utterance is both expensive
and wrong: in a real meeting the bot is silent ~99% of the time.

```
speechTurn (final) ──▶ turn-router policy ──┬─▶ APPEND to running transcript  (default)
                                            └─▶ ESCALATE → deliver as a session
                                                 prompt → LLM decides → reply text
                                                          │
                                                 ┌────────┴─────────┐
                                            speak(text)        (return nothing)
                                                 │                  │
                                            TTS → virtual mic    STAY SILENT
```

- **Default = listen.** Every final turn is appended to a rolling transcript file in
  the session workspace (`/workspace/meeting/<id>/transcript.jsonl`), speaker-tagged.
  No LLM turn is consumed.
- **Escalate** only on a wake condition: the bot is addressed by name / wake-word,
  a direct question is detected, an explicit "@bot do X" connector ask, or a
  host/owner DM. (Phase 1 ships *no* escalation — pure listen + end-of-meeting recap.)
- On escalation the turn (plus recent transcript context) is delivered into the **one
  long-lived opencode session for this meeting** as a follow-up prompt — mechanically
  identical to a Slack thread follow-up (`continueSession` → daemon
  `/session/<id>/prompt_async`). The session's text reply is the candidate to speak.
- **Silence is a first-class output.** The session returning no `speak()` call is a
  valid, common outcome — the router does not force speech.

**Why the agent stays oblivious:** the prompt the session sees is just text — e.g.
`[14:03 Priya] Can the bot pull last week's revenue?` — indistinguishable from a Slack
line. Its reply is plain text. The Channel alone decides text↔audio. The *only* new
instruction in the meet agent's prompt is "you are usually a silent listener; reply
only when addressed or when you have something materially useful."

---

## 2. How a meeting becomes a session

### Trigger: Google Calendar auto-join

There is **no native Google push wiring** in the platform today, and the connector
sweep does not poll external APIs — the idiomatic "poll-and-act" primitive is a **cron
trigger** (60 s scheduler tick, `apps/api/src/projects/lib/triggers.ts`). So:

1. A **1-minute cron trigger** fires a lightweight `meet-scheduler` session.
2. That session calls the **Pipedream `google_calendar`** connector (via the executor
   `call` meta-tool) for events starting in the next ~2 min that carry a Meet link.
   The trigger's `owner_user_id` (`resolveTriggerActor`) selects *whose* calendar/Google
   account resolves — this is the consent + identity chokepoint.
3. For each due meeting, it **fans out one session per meeting**
   (`kortix sessions new --prompt "join <meetUrl> …" --agent meet-bot`), so the
   architectural invariant **one meeting = one session = one sandbox** holds even when
   two meetings start in the same minute. Dedup on `event_id` so a meeting isn't joined
   twice (the scheduler re-fires each minute).
4. Each `meet-bot` session boots the in-sandbox runtime (§3), joins at T-0, and runs
   for the meeting's duration.

> Cron poll is the Phase-1 mechanism (simple, native, no extra infra). A Google
> `events.watch` push channel → `[[triggers]] type="webhook"` is a later optimization;
> the webhook receiver (`projects/routes/r1.ts`) already validates HMAC and would only
> need the watch-channel registered out-of-band. Not worth it for v1.

### `kortix.toml` additions

```toml
# Calendar connector the scheduler reads (1-click connect via the `connect` meta-tool)
[[connectors]]
slug = "google-calendar"
name = "Google Calendar"
provider = "pipedream"
app = "google_calendar"

# 1-min poller: find imminent Meet links, fan out one session per meeting
[[triggers]]
slug = "meet-autojoin"
name = "Auto-join calendar meetings"
type = "cron"
agent = "meet-scheduler"
enabled = true
cron = "0 * * * * *"                 # every minute (6-field croner)
timezone = "America/Los_Angeles"
prompt = """
List my Google Calendar events starting in the next 2 minutes that contain a Google
Meet link. For each one NOT already joined (dedupe on event id under
notes/meet/joined/), spawn a meeting session:
  kortix sessions new --agent meet-bot --prompt "Join the meeting at <meetUrl> for
  calendar event '<title>' (attendees: <names>). Listen and transcribe; do not speak."
Record the event id you joined. If there are none, stop silently.
"""

# The realtime channel surface (inbound speaker turns ↔ outbound speech)
[[channels]]
platform = "meet"
enabled = true
agent = "meet-bot"

# Scope the meeting bot: it may read calendar + use the meet channel; NO write
# connectors in Phase 1 (live actions arrive in Phase 3 behind ask-approval).
[[agents]]
name = "meet-scheduler"
connectors = ["google-calendar"]
kortix_cli = ["project.session.start", "project.session.read"]

[[agents]]
name = "meet-bot"
connectors = ["meet", "google-calendar"]
kortix_cli = ["project.cr.open"]      # to land the recap (Phase 1 deliverable)
```

New OpenCode primitives (land via CR): agents `meet-scheduler.md`, `meet-bot.md`, and
a `kortix-meet` skill (mirrors `kortix-slack`: how to use the `meet` CLI, the
silent-by-default posture, the recap format).

---

## 3. The in-sandbox runtime

One meeting = one sandbox. The sandbox supports Docker-in-DinD, so heavy media deps
are containerized off the agent image. **Pipecat** is the pipeline spine; the browser
bot is the transport edge.

```
                        ┌──────────────── session sandbox (one meeting) ────────────────┐
 Google Meet  ◀──CDP──▶ │  Chromium (headful, Xvfb)  ──page audio──▶  capture           │
   (browser)            │     ▲ virtual mic (Pulse null-sink)              │             │
                        │     │                                            ▼             │
                        │  TTS out ◀── Kokoro/Chatterbox ◀── speak()   faster-whisper STT│
                        │                    ▲                              │ + DOM       │
                        │                    │                              ▼ speaker     │
                        │              RealtimeChannel  ◀── Pipecat frames ─ attribution  │
                        │                    │  ▲                                          │
                        │     speechTurn ───▶│  └─── speak()/silence ◀── turn-router      │
                        │                    ▼                              ▲             │
                        │            `meet` CLI ──/turn-stream──▶ API   opencode session  │
                        └───────────────────────────────────────────────────────────────┘
```

**Wiring:**
- **Browser bot** drives Chromium over CDP. We already ship **`agent-browser`**
  (CDP + accessibility snapshots) in every sandbox — reuse it for navigation/clicks;
  use raw CDP / Playwright only for the media-element + audio-graph work agent-browser
  doesn't cover. Chromium runs **headful under Xvfb** (headless is the #1 bot tell).
- **Audio in:** capture the meeting's combined `<audio>/<video>` MediaStream on the page
  (lifted approach, below) → feed Pipecat → **faster-whisper** STT → segments.
- **Speaker attribution:** a page-injected `MutationObserver` watches participant tiles
  and active-speaker indicators, emitting `SPEAKER_START/END` events with relative
  timestamps; the runtime correlates these with STT segments to produce
  `SpeechTurn.speaker`. (This is exactly vexa's mechanism — lifted.)
- **Audio out (Phase 2+):** `speak()` → Kokoro/Chatterbox TTS → PCM → a **PulseAudio
  `module-null-sink` + virtual source** that Chromium opened as its microphone, so the
  TTS plays into the meeting. Barge-in: `stopSpeaking()` cuts the stream when a human
  starts a turn.
- **Outbound to Kortix:** the `meet` CLI relays transcript/recap/status via
  `/v1/projects/:id/turn-stream` (reusing `chatTurnStreams`), and posts the final recap
  by opening a **CR** (Phase 1) — no token in the sandbox; the gateway resolves it.

### Vexa: LIFT vs REBUILD (per-piece, with file references)

Source: `github.com/Vexa-ai/vexa` → `services/vexa-bot/core/src/` (Apache-2.0). The
production bot is cleanly modularized per platform — `platforms/googlemeet/{join,
admission,recording,removal,leave,selectors}.ts` + `humanized/` + `shared/meetingFlow.ts`.

| Piece | vexa file(s) | Decision | Why |
| --- | --- | --- | --- |
| **DOM selectors** (name input, "Ask to join", Leave, People panel, active-speaker `[data-audio-level]` + obfuscated classes `.Oaajhc/.HX2H7/…`, removal text) | `googlemeet/selectors.ts` | **LIFT (crown jewel)** | This is the brittle, constantly-churning layer. Vendoring it (pinned commit) and re-syncing is *exactly* how we "keep cribbing Vexa's fixes." |
| **Join flow** (locale-agnostic button strategy, disabled-attr removal, value-injection on name field) | `googlemeet/join.ts` | **LIFT, adapt** | Hard-won join heuristics; wrap behind our `join()`. |
| **Admission detection** (waiting-room → "Leave call" present = admitted; timeout) | `googlemeet/admission.ts` (+ `admission.test.ts`) | **LIFT** | Already has tests; maps to `admissionPending/joined/admissionDenied`. |
| **Removal / meeting-end** (People-panel polling ≤1 participant; "Meeting ended"/alert text; `beforeunload`) | `googlemeet/removal.ts`, `leave.ts` | **LIFT** | Maps to `removed`/`ended`. |
| **Audio capture** (combine non-paused `<audio>/<video>` `srcObject` streams → MediaRecorder pipeline; RMS activity gate `AUDIO_ACTIVITY_THRESHOLD`) | `googlemeet/recording.ts` | **LIFT the capture/combine; REBUILD the sink** | Vexa encodes WebM/Opus and **HTTP-uploads chunks to its own meeting-api**. We **rebuild the sink** to feed Pipecat/faster-whisper in-sandbox (raw frames, lower latency). The page-side stream-combine is the reusable nugget. |
| **Speaker attribution** (MutationObserver on tiles; `googleSpeakingIndicators`/class inference; `data-participant-id`; `SPEAKER_START/END` + `relative_timestamp_ms` via exposed callbacks) | `googlemeet/recording.ts` | **LIFT** | The single most valuable non-obvious asset; feeds `SpeechTurn.speaker`. |
| **Anti-bot humanization** (mocap-driven mouse, x11 input) | `googlemeet/humanized/*` | **LIFT selectively** | Optional hardening for join reliability; evaluate vs complexity. |
| **Stealth launch** (Playwright + stealth, headful, navigator overrides) | `core/src/index.ts` | **REBUILD (reference)** | Small; we own browser launch via agent-browser/CDP. Copy the *list* of evasions, not the structure. |
| **Bot orchestration / lifecycle** (`shared/meetingFlow.ts`, `browser-session.ts`, `docker.ts`, BotConfig) | `core/src/*` | **REBUILD** | This is "platform business logic" coupling to Vexa's runtime-api/meeting-api. Replaced by our trigger→session→RealtimeChannel model. |
| **Teams** (native captions) | `platforms/msteams/{captions,join,…}.ts` | **LIFT — Phase 4** | `captions.ts` taps Teams' built-in captions (cheaper than STT). |
| **Zoom** (C++ Meeting SDK wrapper) | `platforms/zoom/native/zoom_wrapper.cpp` | **STUDY — Phase 4** | Zoom is *not* browser-based here; it's the official C++ SDK. Big lift; defer. |

**Vendoring rule:** lifted files live under `packages/realtime-channel/vendor/vexa/`
with a `VEXA_COMMIT` pin and a `SOURCES.md` crediting each file + upstream path +
license. A `scripts/sync-vexa.sh` diffs upstream selectors against the pin so we pull
their fixes deliberately. We **wrap**, never fork-and-drift the non-DOM logic.

---

## 4. The hard 20%, named honestly

These are where meeting-bot projects die. We do not hand-wave them.

1. **Per-platform DOM/join brittleness.** Meet's DOM changes without notice; selectors
   are obfuscated (`.Oaajhc`, `.HX2H7`). Mitigation: centralize **all** selectors in one
   vendored `selectors.ts`, prefer semantic anchors (`aria-label`, `data-audio-level`)
   over class names, keep locale-agnostic fallbacks, and run a **daily canary** that
   joins a throwaway meeting and alerts on selector failure. Crib Vexa via `sync-vexa.sh`.
2. **Audio capture & the virtual mic.** Capture: combining per-element MediaStreams
   reliably, surviving renegotiation/active-speaker DOM swaps, clean PCM at a fixed rate
   for Whisper. Output (Phase 2): a PulseAudio null-sink/virtual-source that Chromium
   actually selects as mic, with no echo/feedback loop, plus barge-in. This is the
   single most fiddly subsystem; budget real time.
3. **Anti-bot detection.** Google actively fingerprints automation. Headful+Xvfb,
   stealth navigator overrides, humanized input, a real Google account in good standing,
   and a believable display name. Expect "Ask to join" gating and occasional hard blocks;
   detect and report rather than silently spin. Accept that this is an arms race.
4. **Admission / removal / end-of-meeting edge cases.** Waiting-room timeouts, host
   never admits, mid-meeting kick, "everyone else left," network drop. Each maps to a
   `RealtimeChannel` event with a defined session response (leave + recap-so-far).
5. **Legal/consent (also a §5 item, but it's a *blocker*, not a nicety).** In two-party-
   consent jurisdictions, recording without disclosure is unlawful. The bot **must**
   disclose before capturing (§5).
6. **Keeping the Vexa lifeline.** Our edge over a from-scratch bot is that Vexa is
   actively maintained and Apache-2.0. The `VEXA_COMMIT` pin + `sync-vexa.sh` diff is a
   first-class, scheduled maintenance task — not a one-time copy.

---

## 5. Governance

- **Consent & disclosure.** The bot joins under a name that *announces itself* —
  e.g. display name `"Kortix Notetaker (recording)"`. On `joined`, it posts a disclosure
  to the meeting chat ("This meeting is being transcribed by an AI assistant on behalf
  of <owner>."), and in Phase 2 speaks a one-line disclosure. Recording is gated on a
  per-project **consent setting**; default conservative (announce + chat disclosure).
  Honor jurisdictional rules (two-party consent, GDPR) — surfaced as a project policy,
  not buried in code.
- **Identity = the trigger owner.** The Google account, calendar, and any connector
  calls resolve to `resolveTriggerActor`'s `owner_user_id`. The agent never exceeds the
  launching user's role (the platform's `agent ≤ user` rule).
- **Per-meeting connector permissions.** The meeting session gets a **scoped** grant via
  `[[agents]] meet-bot` + per-tool policy (`apps/api/src/executor/policy.ts`).
  Phase 1: read-only (calendar + meet channel), **zero write connectors.** Phase 3 live
  actions run **`require_approval` (ask)** by default — the gateway returns
  `pending_approval` and the owner confirms out-of-band before anything mutating happens
  mid-call. Destructive actions are blockable per-meeting.

---

## 6. Phased build plan

Each phase is its own CR with tests (per the testing discipline) and updates this doc.

- **Phase 1 — Join + transcribe + recap (listen-only).**
  - Add `meet` to the outbound channel allow-list + `meet.ts` CLI (auto-shim);
    `meet-scheduler` + `meet-bot` agents; `kortix-meet` skill; the cron + channel +
    connector `kortix.toml` entries.
  - In-sandbox runtime: vendored vexa `googlemeet/` join+admission+recording+removal,
    rebuilt sink → faster-whisper → speaker-attributed `transcript.jsonl`.
  - Calendar fan-out (`meet-autojoin` cron → one session per meeting, event-id dedup).
  - Deliverable: bot joins, transcribes with speaker labels, posts a recap by opening a
    **CR**. No speaking, no live actions.
  - Tests: selector unit tests (lift vexa's `admission.test.ts` pattern), turn-router
    policy unit tests, a contract test for the `meet` channel connector, an integration
    test that drives a mock Meet page through join→transcript.
- **Phase 2 — Speak.** TTS (Kokoro/Chatterbox) → PulseAudio virtual mic; `speak()`/
  `stopSpeaking()` + barge-in; the wake/escalation policy in the turn router; spoken
  disclosure. Deliverable: bot answers when addressed, stays silent otherwise.
- **Phase 3 — Live connector actions.** Escalated turns can call connectors mid-meeting
  under `require_approval`; results spoken/posted. Per-meeting scoped grants + audit.
- **Phase 4 — Zoom / Teams.** Lift `msteams/{join,captions,…}` (native captions beat
  STT); evaluate `zoom/native` C++ SDK vs a browser approach. Both implement the *same*
  `RealtimeChannel` — only the platform edge differs.

---

## Verification (for the implementation phases, not this spec)

- **Spec (this session):** the deliverable is this doc + CR. No runtime verification —
  review is the gate.
- **Phase 1:** `kortix triggers fire meet-autojoin` against a real test calendar event
  with a Meet link; confirm a `meet-bot` session spawns, the bot appears in the meeting,
  `transcript.jsonl` fills with correct speaker labels, and a recap CR opens. Plus the
  unit/contract/integration tests above, green in CI.

---

## Files this work will touch (implementation phases)

- **New OpenCode primitives:** `.kortix/opencode/agents/{meet-scheduler,meet-bot}.md`,
  `.kortix/opencode/skills/kortix-meet/SKILL.md`.
- **New package:** `packages/realtime-channel/` (interface + Meet impl + vendored vexa).
- **Sandbox CLI:** `apps/sandbox/slack-cli/channels/meet.ts` (auto-shimmed).
- **API outbound channel:** `apps/api/src/projects/connectors.ts` (`CHANNEL_PLATFORMS`),
  `apps/api/src/executor/{channels,channel-materialize,db-deps}.ts` (`case 'meet'`).
- **API inbound channel:** `apps/api/src/channels/meet/` (modeled on `telegram-webhook.ts`),
  `session-lifecycle/types.ts` (`'meet'` source), route mounting.
- **Manifest:** `kortix.toml` (connector + cron trigger + channel + `[[agents]]`).
- **Vendoring:** `packages/realtime-channel/vendor/vexa/` + `scripts/sync-vexa.sh`.
- No DB migration (chat tables are already `platform varchar(32)`).
