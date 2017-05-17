# consumer ideas

These are some ideas for approaches on how to implement the partition consumer.

Currently favoring Approach C.

## Approach A: Aquire holds with preference for earliest request, then consume

With three partition replicas P1, P2, P3:

Consumer C requests keys to read from partition P1.

- Request id R is generated.
- Timestamp T is saved.
- P1 finds key K available to read.
- P1 requests a HOLD on K for R,T from P1,P2,P3.
- For each replica:
  - If no HOLD on K exists:
    - Grant HOLD R,T
    - Reply OK
  - If HOLD Rh,Th exists and (R,T) < (Rh,Th)::
    - Replace HOLD Rh,Th with HOLD R,T.
    - Reply OK
  - If HOLD Rh,Th exists and (R,T) < (Rh,Th)::
    - Reply NOK
When P1 has received 2 replies:
  - If OK,OK:
    - Return K to C
  - If any NOK:
    - Return nothing

Consumer C then requests the payload for K in a separate request.

__Strengths:__
- No leader needed
- At least one request succeeds

__Weaknesses:__
- Duplicates on clock skew
- Duplicates on delayed responses
- Duplicates in many other scenarios


## Approach B: Spawn message provider process per partition

With three partition replicas P1, P2, P3:

P nodes elect a leader Pl using Paxos / Raft.
Pl spawns a message provider process MPP.

Consumer C requests for messages are routed to MPP on Pl:

- C requests one message.
- MPP pops message key K from top of buffer.
- MPP fetches payload V for K from Pl or other P node.
- MPP responds with one message K,V.
- If MPP message buffer length is shorter than watermark:
  - Request N keys from P1,P2,P3.
  - For each key K received:
    - Query tombstones (QUORUM read)
    - If no tombstones, enqueue

Also needs to:
- Fetch tombstones for K before returning it. QUORUM fetch.
- Prune buffer as tombstones are written to the node, refill if buffer length shrinks below watermark.
- Keep a list of outstanding messages


__Strengths:__
- Guarantee exactly-once semantics if no node/network issues.
- Can minimize read latencies.

__Weaknesses:__
- Outstanding messages are re-sent if leader node is lost.
- Must implement leader election.
- Multiple tombstone queries per key.

## Approach C: Aquire HOLD majority using consensus, or retry

With three partition replicas P1, P2, P3:

Consumer C requests keys to read from partition P1.

- Oldest key K on P1 with no holds or tombstones is chosen
- HOLD K on P1
- P1 sends HOLD request on K to P2, P3
- For each replica:
  - If tombstone for K exists:
    - Return TOMB, K
  - If no HOLD on K exists:
    - HOLD K for P1
    - Return OK, K
  - If HOLD on K for P1 exists:
    - Update HOLD timeout
    - Return OK, K
  - If HOLD on K for Pn where n != 1 exists:
    - Return NOK, K
- When P1 has received replies or encountered 99p timeout:
  - If any TOMB, K received:
    - Write tombstone for K locally
    - Return nothing
  - If quorum of OK, K achieved:
    - Read value V for K
    - Return K, V
  - Else:
    - Remove HOLD for K on P1
    - (Optional step:) Send requests to remove HOLD K for P1 to all nodes which replied OK
    - Return nothing

Consumer C receives K,V and sends ACK or REJECT:
- ACK:
  - Write tombstones with QUORUM write.
- REJECT:
  - One of:
    - Requeue: Remove all holds for K
    - Deadletter: Write K to deadletter queue, then write tombstones

__Strengths:__
- No leader required.
- No request synchronization required.
- Guarantee exactly-once semantics if client response within timeout and no cluster partitions.
- HOLDs kept in memory, no extra disk pressure.
- No extra consumer processes required.
- Failure states are non-critical, worst case scenario: HOLD deadlock on M until timeout
- API easy to use. Client can query any node and will receive messages with no additional exposed complexity.

__Weaknesses:__
- Message HOLD deadlock possible if replicas > 3 under unstable conditions. In this case, message may need to wait for HOLD timeout before being sent.
- HOLD election can require many retries, meaning increased network traffic and load pressure.
- Client requesting M messages from Pn will receive X messages where 0 <= X <= M even if more than M messages exist in Pn.
- Duplicates may be sent when cluster partitions occur.

__Challenges:__
- Let other partitions know when HOLD majority reached so they stop retrying the key.
- Minimize impact of failure scenarios
- Minimize performance impact of acquiring HOLD majority
- Stay close to Paxos/Raft leader election, do not reinvent any wheels
