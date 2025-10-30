# Dynamic Node Join — Design Options and Implementation Plan

This document summarizes options, trade-offs, and a minimal implementation plan to allow nodes to join the cluster dynamically (discover/announce themselves) instead of requiring a full hard-coded peer list.

Goals
- Allow new nodes to join a small LAN cluster with minimal operator steps.
- Avoid making risky protocol-level changes to Raft membership semantics in this initial phase.
- Provide practical, incremental approaches: quick experiment-friendly options and notes for production-grade membership changes.

Top-level options (increasing complexity & correctness)

1) Bootstrap Join + Best-effort Gossip (Suggested immediate option)
   - Workflow:
     - New node connects to a provided bootstrap node address and sends a `NodeJoin` handshake containing its id and advertised listen address.
     - Bootstrap replies with a `NodeList` containing known peers.
     - New node connects to those peers and starts normal operation.
     - Bootstrap broadcasts the `NodeJoin` to its peers so they can learn and optionally connect back.
   - Pros: Minimal invasive changes, easy to implement and test, works well on trusted LANs.
   - Cons: Not Raft-safe for membership changes (no joint-consensus); best for experiments/dev/labs.

2) mDNS / UDP Broadcast Discovery (LAN plug-and-play)
   - Workflow:
     - Nodes broadcast presence via mDNS or UDP multicast on startup.
     - Receiving nodes reply with a list of known peers or a bootstrap pointer.
     - New node uses reply to bootstrap and connect.
   - Pros: Zero config on LAN, good UX.
   - Cons: Platform/network details (mDNS differences), still needs membership-change semantics for correctness, security concerns.

3) Correct Raft Membership Changes (Joint Consensus) — Production-grade
   - Workflow:
     - Implement membership-change protocol using joint-consensus as specified in Raft (or migrate to `openraft` / `async-raft`).
     - Membership changes are replicated through the Raft log and committed using majority rules.
   - Pros: Correct and robust for production clusters.
   - Cons: Non-trivial to implement; consider migration to an established Raft crate that supports membership changes.

Design choice recommended for this repo
- Implement option (1) — Bootstrap Join + Best-effort Gossip — as a practical first step.
- Document and label it clearly as experimental / not Raft membership-safe.

Protocol additions (minimal)
- Extend the control message set with two lightweight messages:
  - `NodeJoin { node_id: u32, addr: String }`
  - `NodeList { peers: Vec<(u32, String)> }`

Join handshake (new node)
1. New node binds its listen address.
2. If `--bootstrap` provided, connect to that addr and send `NodeJoin{ id, addr }`.
3. Read `NodeList` response. Merge peers into local peers map and spawn outbound connects to them.

Bootstrap handling (existing node)
1. On receiving `NodeJoin` control message:
   - Add the new peer to the local peers map (best-effort).
   - Reply with `NodeList` containing known peers.
   - Broadcast `NodeJoin` to existing peers so they can learn the newcomer.

Implementation notes
- Handshakes should be encoded as normal Raft frames (the same length-delimited JSON frames) before starting normal Raft RPCs on that stream.
- Keep this behavior optional: allow `--peers` to remain as the default configuration method; only perform bootstrap when `--bootstrap` is given.
- Keep a simple dedupe logic (if id already known, update address and close old connection if different).
- Avoid changing Raft's commit/replication logic — this is a discovery/connection convenience only.

Edge cases and caveats
- Concurrent joins and leader election during join can lead to inconsistent partial views — acceptable for experiments but not production.
- Security: add an optional shared token or signed handshake if operating on untrusted LANs.
- NATs and firewalls: bootstrap must be reachable by the joining node.

Testing plan
- Unit tests for handshake serialization/deserialization.
- Integration test (local): start a bootstrap node, then start two newcomers with `--bootstrap` and assert they connect and exchange NodeJoin/NodeList messages (inspect logs).

Operational commands (example)
- Start bootstrap node (node 1):
  ```bash
  ./run_node.sh 1 192.168.1.10:7001 ""
  ```
- Start node 2 and join via bootstrap:
  ```bash
  ./run_node.sh 2 192.168.1.11:7002 "" --bootstrap 192.168.1.10:7001
  ```

Follow-ups
- If you want Raft-correct membership changes, I can draft a migration plan to `openraft` and implement a small POC.
- If you want zero-config LAN discovery, I can implement an mDNS-based discovery module (requires more platform testing).

---

Document created by the automated assistant on behalf of the project maintainers. Use this as a reference when implementing dynamic joining or discussing membership-change upgrades.
