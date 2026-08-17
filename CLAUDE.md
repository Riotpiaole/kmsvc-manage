# kmsvc Architecture Clarifications

## Inter-Pod Message Brokering (Parallel Processing)

**kmsvc is a distributed, inter-pod SQS-compatible message queue service.** Multiple pods run in parallel sharing the same Kafka cluster and Redis coordination layer.

### Message Flow
```
Client Service Pod 1 ----\
Client Service Pod 2 ------ gRPC/REST → kmsvc Service (N replicas, stateless)
Client Service Pod N ----/                    ↓
                                           Kafka (3 brokers, KRaft)
                                              ↓
                                          Redis (state coordination)
```

### Key Facts
1. **Kafka is the message broker** (durable, distributed) — not in-memory, not per-pod
2. **Redis coordinates** in-flight state, dedup, FIFO gating via atomic Lua ops—no leader election
3. **Service replicas are stateless** — any replica can handle send/receive/delete
4. **Consumer groups** (Kafka native) + Redis low-watermark strategy = at-least-once delivery
5. **External services** can:
   - Call kmsvc gRPC/REST API (REST via ingress-nginx)
   - Produce/consume Kafka topics directly (same cluster)

### Design References
- `design.md §1`: Architecture diagram
- `design.md §2`: API surface (CRD for lifecycle, gRPC/REST for messages)
- `design.md §3`: Offset-commit + FIFO gating (inter-pod coordination)
- `design.md §9`: Multi-replica scaling (no leader needed)

### Operational
- Horizontal scaling: add more `kmsvc` service replicas—Kafka rebalances automatically
- HA: Redis Sentinel/Cluster recommended for production (design.md §9) — currently standalone
- Monitoring: Kafka consumer-group lag, Redis pending/inflight keys, visibility timeouts

## Temporal Namespace Registration — Always Automatic, Never Manual

**Never manually run `temporal operator namespace create` (or the CLI/UI equivalent) for a namespace that a Queue's `temporal.io/namespace` label will reference.** `queue-operator`'s `reconcileTemporalWorker` (`internal/operator/queue_controller.go`) registers the Temporal namespace itself — idempotently, via `TemporalNamespaceRegisterer.RegisterNamespace` (`internal/operator/temporal_namespace.go`, real impl in `internal/temporal/client.go`) — before creating the `TemporalWorker` CR. This runs on every reconcile of every Queue carrying that label, so the namespace and its worker always exist together.

**The only steps to bring up a new Temporal namespace + worker are:**
1. Apply a `Queue` CR with `metadata.labels["temporal.io/namespace"] = "<namespace>"`.
2. That's it. `queue-operator` registers the namespace, creates `TemporalWorker/worker-<namespace>` in the `temporal` namespace, and its backing Deployment.

**Why this matters:** before this existed, a Queue's `temporal.io/namespace` label was trusted as-is with no verification — a typo'd or never-registered namespace silently produced a worker pod polling a namespace that doesn't exist, with no error surfaced anywhere until someone noticed workflows never executing. Manually pre-creating the namespace masks this — don't do it, let the operator own it.

One `TemporalWorker` per Temporal namespace serves *all* Queues labeled with that namespace (not one worker per Queue) — see the type doc on `TemporalWorkerSpec` in `apis/kmsvc/v1/temporalworker_types.go`.
