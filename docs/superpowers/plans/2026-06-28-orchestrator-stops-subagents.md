# Orchestrator Stops Subagents — Cancellation Propagation

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** When a parent session (orchestrator) is cancelled, all running descendant subagent sessions are also cancelled.

**Architecture:** `SessionRunState.cancel(sessionID)` currently cancels only the targeted session's runner and its background jobs. It does not walk the session tree to cancel child/descendant runners. We add `Session.Service` to `SessionRunState` and a `cancelDescendants` helper that recursively finds child sessions via the existing `Session.children()` DB query and cancels each child's runner and background jobs.

**Tech Stack:** Effect-TS, TypeScript, SQLite (session tree via `parent_id` column)

---

### Root Cause

`packages/opencode/src/session/run-state.ts:76-85` — `SessionRunState.cancel`:

```typescript
const cancel = Effect.fn("SessionRunState.cancel")(function* (sessionID: SessionID) {
  yield* cancelBackgroundJobs(background, sessionID)
  const data = yield* InstanceState.get(state)
  const existing = data.runners.get(sessionID)
  if (!existing || !existing.busy) {
    yield* status.set(sessionID, { type: "idle" })
    return
  }
  yield* existing.cancel
})
```

This cancels only `sessionID`'s own runner and background jobs. Foreground subagents each have their own sessions with runners registered via `ensureRunning`, but they are never found or cancelled.

The existing `onAbort` listener in `packages/opencode/src/tool/task.ts:344-346` provides a secondary cancellation path that fires the `taskAbort` signal when the parent's fiber is interrupted, but this only works while the task tool's `run()` function is actively executing within a tool call. When a user cancels the orchestrator session via the CLI or HTTP API, `SessionPrompt.cancel` is called directly, which calls `state.cancel(orchestratorSessionID)` — the `onAbort` path is not guaranteed to fire for all in-flight subagents before the orchestrator's fiber tears down.

### Fix Overview

1. Add `Session.Service` as a dependency to `SessionRunState.layer`
2. Add a `cancelDescendants` helper that queries `Session.children(sessionID)` and recursively calls `cancel` on each child
3. Call `cancelDescendants` inside `cancel` before cancelling the session's own runner
4. Update the `Interface` if needed (no change — `cancel(sessionID)` signature stays the same)
5. Add tests

---

### Files to Modify

- **Modify:** `packages/opencode/src/session/run-state.ts` — add `Session.Service` dependency, add `cancelDescendants`, call it from `cancel`
- **Modify:** `packages/opencode/src/session/run-state.ts` — add `Session` to the imports
- **Create:** `packages/opencode/test/session/run-state.test.ts` — tests for descendant cancellation

### Files Status Check (no changes needed)

- `packages/opencode/src/session/session.ts` — `children(parentID)` already exists and queries by `parent_id` (line 629)
- `packages/opencode/src/session/prompt.ts` — `cancel` calls `state.cancel(sessionID)` → the fix propagates from there
- `packages/opencode/src/tool/task.ts` — the `onAbort` path becomes a secondary safety net; no change needed
- `packages/opencode/src/kilocode/session/index.ts` — the `parents` in-memory map already tracks parent→child; but using `Session.children` (SQL) is more reliable since it works across process restarts

---

### Task 1: Add session dependency and `cancelDescendants` to `SessionRunState`

**Files:**
- Modify: `packages/opencode/src/session/run-state.ts`

The `SessionRunState` layer currently takes `BackgroundJob.Service` and `SessionStatus.Service`. We add `Session.Service` so `cancel` can query children.

- [ ] **Step 1: Add `Session` import and layer dependency**

In `packages/opencode/src/session/run-state.ts`, add `Session` to the imports:

```typescript
import * as Session from "./session"
```

In the `layer` effect, add `Session.Service` to the yield* block:

```typescript
const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const background = yield* BackgroundJob.Service
    const status = yield* SessionStatus.Service
    const sessions = yield* Session.Service    // ← ADD

    const state = yield* InstanceState.make(
      // ...rest unchanged
```

- [ ] **Step 2: Add the `cancelDescendants` helper function**

After the `cancelBackgroundJobs` function (after line 147), add:

```typescript
const cancelDescendants = Effect.fn("SessionRunState.cancelDescendants")(function* (
  sessions: Session.Interface,
  background: BackgroundJob.Interface,
  sessionID: SessionID,
) {
  const kids = yield* sessions.children(sessionID)
  for (const kid of kids) {
    yield* cancelAll(kid.id)
  }
})

const cancelAll: (
  sessions: Session.Interface,
  background: BackgroundJob.Interface,
  sessionID: SessionID,
) => Effect.Effect<void> = Effect.fn("SessionRunState.cancelAll")(function* (
  sessions,
  background,
  sessionID,
) {
  yield* cancelBackgroundJobs(background, sessionID)
  yield* cancelDescendants(sessions, background, sessionID)
  const data = yield* InstanceState.get(state)
  const existing = data.runners.get(sessionID)
  if (!existing || !existing.busy) return
  yield* existing.cancel
})
```

**Explanation:** `cancelAll` is the recursive workhorse. For a given session, it:
1. Cancels background jobs for that session
2. Recursively cancels all descendants
3. Cancels the session's own runner (if busy)

This order ensures children are stopped before the parent, which prevents races where the parent could spawn more children during teardown.

- [ ] **Step 3: Refactor `cancel` to use `cancelAll`**

Replace the existing `cancel` function:

```typescript
const cancel = Effect.fn("SessionRunState.cancel")(function* (sessionID: SessionID) {
  yield* cancelAll(sessions, background, sessionID)
  // After cancelling everything, set status to idle for the root session
  const data = yield* InstanceState.get(state)
  const existing = data.runners.get(sessionID)
  if (!existing?.busy) {
    yield* status.set(sessionID, { type: "idle" })
  }
})
```

Note: `cancelAll` already cancels the runner for `sessionID`, so we only need the idle status set for the root. However, we also need to not double-cancel. Let me revise:

Actually, `cancelAll` already calls `existing.cancel` which transitions the runner to idle. But the `cancel` function (as the public API) should still set the idle status. Let me simplify — keep `cancelAll` as the internal recursive function, and have `cancel` call `cancelAll` then handle the root status:

```typescript
const cancelAll = Effect.fn("SessionRunState.cancelAll")(function* (
  sessionID: SessionID,
) {
  yield* cancelBackgroundJobs(background, sessionID)
  const kids = yield* sessions.children(sessionID)
  for (const kid of kids) {
    yield* cancelAll(kid.id)
  }
  const data = yield* InstanceState.get(state)
  const existing = data.runners.get(sessionID)
  if (!existing?.busy) return
  yield* existing.cancel
})

const cancel = Effect.fn("SessionRunState.cancel")(function* (sessionID: SessionID) {
  yield* cancelAll(sessionID)
  const data = yield* InstanceState.get(state)
  const existing = data.runners.get(sessionID)
  if (!existing?.busy) {
    yield* status.set(sessionID, { type: "idle" })
  }
})
```

- [ ] **Step 4: Provide `Session.defaultLayer` in the default layer chain**

In the `defaultLayer` export:

```typescript
export const defaultLayer = layer.pipe(
  Layer.provide(BackgroundJob.defaultLayer),
  Layer.provide(SessionStatus.defaultLayer),
  Layer.provide(Session.defaultLayer),    // ← ADD
)
```

- [ ] **Step 5: Run the existing tests to verify no regressions**

```bash
cd packages/opencode && bun test test/session/run-state 2>&1 || echo "no run-state test file yet"
cd packages/opencode && bun test test/tool/task.test.ts 2>&1
```

Expected: All existing task tests pass.

---

### Task 2: Write tests for descendant cancellation

**Files:**
- Create: `packages/opencode/test/session/run-state.test.ts`

- [ ] **Step 1: Create test file with the layer setup**

```typescript
import { describe, expect, afterEach } from "bun:test"
import { Effect, Layer } from "effect"
import { BackgroundJob } from "../../src/background/job"
import { SessionRunState } from "../../src/session/run-state"
import { SessionStatus } from "../../src/session/status"
import { Session } from "../../src/session/session"
import { SessionID } from "../../src/session/schema"
import { RuntimeFlags } from "../../src/effect/runtime-flags"
import { Config } from "../../src/config/config"
import { Bus } from "../../src/bus"
import { Agent } from "../../src/agent/agent"
import { Provider } from "../../src/provider/provider"
import { Truncate } from "../../src/tool/truncate"
import { ToolRegistry } from "../../src/tool/registry"
import * as CrossSpawnSpawner from "@opencode-ai/core/cross-spawn-spawner"
import { disposeAllInstances } from "../fixture/fixture"
import { testEffect } from "../lib/effect"

const it = testEffect(
  Layer.mergeAll(
    BackgroundJob.defaultLayer,
    Bus.defaultLayer,
    Session.defaultLayer,
    SessionRunState.defaultLayer,
    SessionStatus.defaultLayer,
    Config.defaultLayer,
    RuntimeFlags.layer(),
    Agent.defaultLayer,
    CrossSpawnSpawner.defaultLayer,
    Provider.defaultLayer,
    Truncate.defaultLayer,
    ToolRegistry.defaultLayer,
  ),
)

afterEach(async () => {
  await disposeAllInstances()
})

const seed = Effect.fn("RunStateTest.seed")(function* () {
  const sessions = yield* Session.Service
  const parent = yield* sessions.create({ title: "Parent" })
  const child = yield* sessions.create({ parentID: parent.id, title: "Child" })
  const grandchild = yield* sessions.create({ parentID: child.id, title: "Grandchild" })
  return { parent, child, grandchild }
})
```

- [ ] **Step 2: Write the first test — cancelling a leaf session**

```typescript
it.live("cancelling a leaf session does not affect siblings", () =>
  Effect.gen(function* () {
    const sessions = yield* Session.Service
    const { parent, child, grandchild } = yield* seed()
    // Create a second child as a sibling
    const sibling = yield* sessions.create({ parentID: parent.id, title: "Sibling" })

    const runState = yield* SessionRunState.Service
    yield* runState.cancel(grandchild.id)

    const all = yield* sessions.children(parent.id)
    // Sibling should still be alive
    const siblingAfter = all.find((s) => s.id === sibling.id)
    expect(siblingAfter).toBeDefined()

    // Grandchild should no longer have a busy runner (it was never started, just checking no error)
    const gcAfter = yield* sessions.get(grandchild.id)
    expect(gcAfter).toBeDefined() // session record still exists
  }),
)
```

- [ ] **Step 3: Write the core test — cancelling parent recursively cancels all descendant runners**

```typescript
import { Runner } from "../../src/effect/runner"
import { MessageV2 } from "../../src/session/message-v2"

it.live("cancelling a parent session cancels all descendant runners", () =>
  Effect.gen(function* () {
    const sessions = yield* Session.Service
    const runState = yield* SessionRunState.Service
    const { parent, child, grandchild } = yield* seed()

    // Start runners for all three sessions
    const latchChild = yield* Effect.latch()
    const latchGrandchild = yield* Effect.latch()

    const startRunner = (id: SessionID, latch: Effect.Latch.Latch) =>
      Effect.gen(function* () {
        yield* runState.ensureRunning(
          id,
          Effect.void,
          Effect.never.pipe(Effect.onInterrupt(() => latch.open)),
        )
      }).pipe(Effect.fork)

    // Start grandchild first, then child (so they're both running when we cancel parent)
    const f1 = yield* startRunner(grandchild.id, latchGrandchild)
    const f2 = yield* startRunner(child.id, latchChild)
    // Small yield to let fibers start
    yield* Effect.sleep("10 millis")

    // Cancel the parent
    yield* runState.cancel(parent.id)

    // Both descendant runners should be interrupted
    expect(yield* latchGrandchild.await).toBe(true)
    expect(yield* latchChild.await).toBe(true)
  }),
)
```

- [ ] **Step 4: Run the tests**

```bash
cd packages/opencode && bun test test/session/run-state.test.ts
```

Expected: All tests PASS.

- [ ] **Step 5: Commit**

```bash
git add packages/opencode/src/session/run-state.ts
git add packages/opencode/test/session/run-state.test.ts
git commit -m "fix: propagate session cancellation to all descendant subagent runners

When SessionRunState.cancel(sessionID) is called (e.g. when the
orchestrator is stopped by the user), it now recursively finds and
cancels all child/descendant sessions' runners and background jobs.

Previously only the targeted session's runner and its direct background
jobs were cancelled. Foreground subagents' runners remained active,
continuing to consume tokens until they completed naturally.

Fixes Kilo-Org/kilocode#11758"
```

---

### Task 3: Update existing integration tests if needed

**Files:**
- Modify: `packages/opencode/test/kilocode/server/listener-runtime.test.ts` — if it provides `SessionRunState.layer` without `Session.defaultLayer`

- [ ] **Step 1: Check if any existing test providers need updating**

Check all usages of `SessionRunState.defaultLayer` or `SessionRunState.layer`:

```bash
cd packages/opencode && rg "SessionRunState\.(defaultLayer|layer)" test/ --files-with-matches
```

For each file, verify the layer chain includes `Session.defaultLayer`. If not, add it:

```typescript
Layer.provide(Session.defaultLayer)
```

- [ ] **Step 2: Run the full test suite**

```bash
cd packages/opencode && bun test
```

Expected: All tests PASS. If any fail, add the missing `Session.defaultLayer` provision.

- [ ] **Step 3: Commit any test layer fixes**

```bash
git add -A
git commit -m "test: provide Session.defaultLayer where SessionRunState is used"
```

---

### Edge Cases

1. **Session with no children** — `sessions.children(sessionID)` returns `[]`, `cancelAll` skips the loop. No change in behavior.

2. **Session already removed** — `sessions.children(sessionID)` may fail with `NotFoundError`. The `cancel` function catches this via existing error handling. If not, wrap the children query:

```typescript
const kids = yield* sessions.children(sessionID).pipe(
  Effect.catchTag("NotFoundError", () => Effect.succeed([])),
)
```

3. **Runner already cleaned up** — `data.runners.get(sessionID)` returns `undefined`, `cancelAll` returns early since there's nothing to cancel.

4. **Circular parent references** — Not possible; `Session.children` queries by `parent_id` in SQLite, which is a flat FK column. No cycles.

5. **Background subagents** — Already handled by `cancelBackgroundJobs` which is called in `cancelAll`. The recursive walk also catches any background-only subagents that were missed.

6. **Deep nesting** — The recursive approach handles arbitrary depth. For a session tree with depth N, `cancelAll` makes N+1 `Session.children` queries and cancels runners depth-first. This is acceptable since session trees are typically shallow (1-3 levels).
