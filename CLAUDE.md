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
