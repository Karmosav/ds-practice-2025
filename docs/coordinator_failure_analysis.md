# Coordinator Failure in the Commitment Protocol

## Context
In this system, the coordinator role is executed by the current leader replica of the executor service. The coordinator drives the two-phase commit (2PC) interaction with participants (books database and payment).

A coordinator failure during 2PC is a critical fault because participants may already be in a prepared state and cannot safely decide alone.

## Failure Windows and Consequences

### 1. Coordinator crashes before any participant receives Prepare
- No participant state change has happened.
- The transaction can be retried safely.

### 2. Coordinator crashes after one or more participants prepared, before writing/sending final decision
- Some participants are in uncertain prepared state.
- Resources may remain reserved/locked.
- This is the classic blocking behavior of 2PC.

### 3. Coordinator crashes after deciding Commit, but before notifying all participants
- A subset of participants may have committed.
- Others may still be prepared.
- Temporary cross-service inconsistency appears until recovery/replay.

### 4. Coordinator crashes after deciding Abort, but before notifying all participants
- Some participants may remain prepared while others already aborted.
- Resources may stay reserved until decision replay reaches all participants.

## Why This Happens
2PC guarantees atomicity only if the coordinator decision is recoverable and replayable. Without durable decision state, a new coordinator cannot distinguish:
- transactions that should be committed,
- transactions that should be aborted,
- transactions still waiting for votes.

## Proposed Solution

### A. Durable Coordinator Write-Ahead Log
Persist transaction state transitions before sending network messages:
1. STARTED(order_id)
2. PREPARE_SENT / VOTES_COLLECTED
3. DECISION_COMMIT or DECISION_ABORT
4. OPTIONAL: PARTICIPANT_ACKS

Key rule: final decision must be written to durable storage before broadcasting Commit/Abort.

### B. Leader Epoch/Fencing
Attach an epoch/term to coordinator authority.
- Only the active leader term can issue decisions.
- Participants reject stale coordinator terms.

This prevents split-brain decisions by old leaders after failover.

### C. Participant Decision Persistence
Each participant persists:
- prepared transactions,
- committed transactions,
- aborted transactions.

On restart, participant restores this state and responds idempotently.

### D. New-Leader Recovery Replay
After leader failover:
1. New leader loads coordinator log.
2. For each in-flight transaction:
   - If DECISION_COMMIT exists, replay Commit to all participants until acknowledged.
   - If DECISION_ABORT exists, replay Abort to all participants until acknowledged.
   - If no final decision exists, apply presumed-abort and replay Abort.

### E. Timeout-Based Resolution Path
Prepared participants that wait too long should:
1. Query coordinator for decision.
2. If coordinator unreachable, query newly elected leader.
3. Continue waiting or apply only a policy-consistent safe default (typically presumed-abort unless durable commit decision is known).

## Safety and Liveness Justification

### Safety (Atomicity)
- A transaction commits only if commit decision is durably recorded and replayable.
- Participants process duplicate Commit/Abort idempotently.
- No participant should commit based only on local timeout.

### Liveness (Eventual Completion)
- New leader eventually replays a final decision for all uncertain transactions.
- Prepared participants do not remain blocked indefinitely once coordinator recovery/failover finishes.

## Suggested Minimal Integration Steps
1. Add a persistent coordinator transaction log to executor leader logic.
2. Add coordinator term/epoch checks in participant RPC requests.
3. Ensure both participants persist prepared and final decision states.
4. Implement a startup recovery loop in the new coordinator leader to replay unresolved transactions.
5. Keep Commit and Abort idempotent in all participants.

## Conclusion
Coordinator failure is the main blocking risk in 2PC. The correct mitigation is durable decision logging plus failover replay, combined with idempotent participant state machines and leader fencing. This preserves atomicity while restoring progress after coordinator crashes.
