# pessi-raft

This project was created as part of a university course on Fault Tolerance. The deliverables included implementing a functional Raft node compatible with [Maelstrom](https://github.com/jepsen-io/maelstrom), identifying Byzantine faults in the base Raft protocol, and proposing and implementing protocol modifications to address these issues.

We created a Raft implementation in Rust that works with Maelstrom. To help spot and fix protocol problems, we added a fault injection system, basically, a way to plug in custom code that simulates different types of faults by reacting to certain Raft events.

To evaluate our changes, we ran scenarios in which some nodes behaved maliciously, systematically checking if the Raft protocol properties (Election Safety, Log Safety, Election Liveness, and Log Liveness) hold. We performed these tests both before and after applying our protocol fixes, allowing us to compare the results and verify whether the modifications were effective in every explored state space using [Stateright](https://github.com/stateright/stateright).

## Fault Injection Experiments
Each fault injector demonstrates a specific kind of safety or liveness failure and, where applicable, a mitigation. See the detailed rationale and sequences in `report/report.pdf` and `report/presentation.pdf` (although in Portuguese).

- Message Forgery (`src/fault/message_forgery.rs`)
  - Forged `VoteResponse` messages allow electing multiple leaders in the same term
  - Violates: Election Safety
  - Mitigation (conceptual): authenticated messages (digital signatures)

- Double Vote (`src/fault/double_vote.rs`) and Fix (`src/fault/double_vote_fix.rs`)
  - A node votes for multiple candidates in the same term
  - Violates: Election Safety
  - Fix: broadcast `ElectedBy { leader, term, by }` after election; detect duplicate voters and blacklist them
  - Caveat: robust authenticity would need signatures (not implemented)

- Election Spam DoS (`src/fault/election_spam.rs`) and Fix (`src/fault/election_spam_fix.rs`)
  - Malicious nodes continuously trigger elections, preventing stable leadership
  - Violates: Log Liveness
  - Fix: heuristic rate-limiting by blacklisting nodes that initiate elections in too many consecutive terms

- Forking Log (`src/fault/forking_log.rs`)
  - Mutates `LogRequest` suffix to diverge followers’ logs
  - Violates: Log Safety
  - Mitigation (discussed): hash-based cross-checking and blacklisting; trade-offs and new attack vectors → not implemented

- Fake Commit (`src/fault/fake_commit_log.rs`)
  - Leader increments commit without quorum
  - Risks: acknowledged-but-lost writes after leader change
  - Mitigation (conceptual): require quorum-signed acks to advance commit; not implemented


## How to run

### Prerequisites
- **Rust**
- **Maelstrom**: Java 11+ and the Maelstrom CLI/JAR

### Build
```bash
cargo build --release
```

### Run with Maelstrom (linearizable KV over Raft)
This node speaks Maelstrom’s `lin-kv` protocol (`read`, `write`, `cas`). Example runs:

- 3 nodes, modest load:
```bash
maelstrom test \
  -w lin-kv \
  --bin target/release/pessi-raft \
  --node-count 3 \
  --time-limit 20 \
  --rate 10 \
  --concurrency 2n \
  --log-stderr --log-net -v
```

### Run the test suite
```bash
cargo test
```

### Run individual fault experiments (property checks)
Each experiment is encoded as a test that drives a Stateright model:

- Double Vote fault (should violate Election Safety):
```bash
cargo test --release test_double_vote_fault
```

- Election Spam fault (should violate Log Liveness):
```bash
cargo test --release test_election_spam
```

- Double Vote fix (should satisfy properties in the model):
```bash
cargo test --release test_double_vote_fix
```

- Election Spam fix (should satisfy properties in the model):
```bash
cargo test --release test_election_spam_fix
```

### Optional: Stateright Explorer UI
There are commented `.serve("localhost:3000")` lines in several tests (e.g., in `src/fault/double_vote_fix.rs`, `src/fault/election_spam.rs`, `src/fault/actor.rs`). To use the web UI:

1. Uncomment the `.serve("localhost:3000")` line in the test you want to visualize or add it if that test doesn't have it.
2. Comment out the corresponding `.spawn_*().join()` call for that test.
3. Run the test so it starts the server (it will block):
```bash
cargo test --release
```
4. Open `http://localhost:3000` in your browser to explore states, traces, and counterexamples.



