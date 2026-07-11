# Temporal + kmsvc Integration Design

## Vision
Use **kmsvc Queue CRDs** as the source of truth for Temporal worker provisioning. When a Queue is created, the Temporal worker operator automatically spawns a corresponding worker Deployment that listens to the queue's task queue.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  Developer: kubectl apply Queue CRD                             │
│  (queue: orders-fifo, fifo: true, partitions: 6)                │
└──────────────────────┬──────────────────────────────────────────┘
                       │
                       v
        ┌─────────────────────────────────────--─┐
        │  TemporalWorker Operator (kmsvc-manage)|
        │  • Watches Queue CRDs                  │
        │  • Detects temporal.io/enabled label   │
        │  • Creates/scales Deployments          │
        └──────────────────────────────────────--┘
                       │
        ┌──────────────┴──────────────┐
        │                             │
        v                             v
   ┌─────────────────┐        ┌─────────────────┐
   │ Temporal Worker │        │ Temporal Worker │
   │  Deployment     │  ...   │  StatefulSet    │
   │ (Replicas: N)   │        │ (DLQ processor) │
   └────────┬────────┘        └────────┬────────┘
            │                          │
            └──────────────┬──────────-┘
                           │
                    ┌──────v───────-┐
                    │  Temporal     │
                    │  Frontend     │
                    │  (Task Queues)│
                    └───────────────┘
```

## CRD: TemporalWorker

**Namespace:** `temporal` (co-located with Temporal cluster)

```yaml
apiVersion: temporal.kmsvc.io/v1
kind: TemporalWorker
metadata:
  name: worker-orders-fifo          # derived from Queue name + suffix
  namespace: temporal
  ownerReferences:
    - apiVersion: kmsvc.io/v1
      kind: Queue
      name: orders-fifo              # 1:1 reference to source Queue
      uid: <uuid>
spec:
  # Source queue (read-only, set by operator)
  queueRef:
    name: orders-fifo
    namespace: sqs                   # Queue lives in sqs namespace

  # Temporal task queue name (defaults to Queue name if omitted)
  taskQueueName: orders-fifo         # Temporal sees this task queue
  
  # Worker deployment config
  image: story-crater-backend:latest  # must have Temporal SDK initialized
  imagePullPolicy: IfNotPresent
  
  # Replica count (can be overridden per-queue)
  replicas: 2                        # default: 1, autoscale later
  
  # Resource constraints
  resources:
    requests:
      cpu: 500m
      memory: 512Mi
    limits:
      cpu: 2000m
      memory: 2Gi
  
  # Pod placement
  nodeSelector: {}                   # default: any worker node
  affinity: {}                       # optional: custom affinity rules
  tolerations: []
  
  # Lifecycle hooks (optional)
  lifecycle:
    postStartCommand: []             # e.g., ["register-activities.sh"]
    preStopCommand: []               # e.g., ["drain-in-flight.sh"]

status:
  phase: Ready                        # Pending | Ready | Failed
  replicas: 2
  readyReplicas: 2
  conditions:
    - type: WorkerDeploymentReady
      status: "True"
      lastTransitionTime: "2026-07-10T12:34:56Z"
      reason: DeploymentReady
      message: "Worker Deployment worker-orders-fifo is running 2/2 replicas"
    - type: TemporalTaskQueueRegistered
      status: "True"
      lastTransitionTime: "2026-07-10T12:34:56Z"
      reason: QueueAvailable
      message: "Task queue 'orders-fifo' is available in Temporal Frontend"
```

## Implementation Roadmap

### Phase 1 (MVP): Manual TemporalWorker CRD (user creates explicitly)
1. Define `TemporalWorker` CRD in Go + Kubernetes schema
2. Implement controller that watches TemporalWorker objects
3. For each TemporalWorker:
   - Create a Deployment with the specified image/replicas/resources
   - Pod template includes:
     - Environment variables: `TEMPORAL_FRONTEND_ADDRESS`, `TEMPORAL_TASK_QUEUE`, `TEMPORAL_NAMESPACE`
     - Init container: wait for Temporal Frontend to be ready (DNS check: `temporal-frontend.temporal.svc.cluster.local:7233`)
   - Set owner reference back to the source Queue (for cleanup on Queue deletion)
4. Create example TemporalWorker CRD (e.g., `k8s/temporal/workers/worker-orders-fifo.yaml`)
5. Deploy via helmfile postsync hook: `kubectl apply -f k8s/temporal/workers/`

### Phase 2 (Future): Auto-provisioning from Queue CRDs
1. Extend kmsvc Queue CRD with optional label: `temporal.io/worker-enabled: "true"`
2. Extend queue-operator to watch Queue CRDs
3. On Queue creation with the label, auto-create a TemporalWorker CRD
4. Auto-derived fields:
   - `taskQueueName` = Queue name
   - `image` = default worker image (from configurable CM or env var)
   - `replicas` = default (e.g., 1, or derived from Queue.spec.partitionsPerShard)

### Phase 3 (Future): Autoscaling
1. Operator samples Queue depth via `kmsvc.io/metrics` endpoints
2. Adjust TemporalWorker replicas based on lag (similar to HPA but custom logic)
3. Min/max replicas configurable per worker

## Integration Points

### 1. Deployment Pod Template (What workers run)

The application container must:
- Initialize Temporal SDK worker: `go.temporal.io/sdk/worker.New(...)`
- Register activity & workflow functions with the worker
- Listen on task queue = TemporalWorker.spec.taskQueueName (env var: `TEMPORAL_TASK_QUEUE`)
- Connect to Temporal Frontend: `TEMPORAL_FRONTEND_ADDRESS=temporal-frontend.temporal.svc.cluster.local:7233`

**Example (story-crater-backend):**
```go
package main

import (
  "fmt"
  "os"
  "go.temporal.io/client"
  "go.temporal.io/sdk/worker"
)

func main() {
  // Read from TemporalWorker env vars
  frontendAddr := os.Getenv("TEMPORAL_FRONTEND_ADDRESS")  // temporal-frontend.temporal:7233
  taskQueue := os.Getenv("TEMPORAL_TASK_QUEUE")            // orders-fifo
  namespace := os.Getenv("TEMPORAL_NAMESPACE")             // default

  c, _ := client.Dial(client.Options{
    HostPort: frontendAddr,
  })
  defer c.Close()

  w := worker.New(c, namespace, taskQueue, worker.Options{})
  
  // Register activities & workflows
  w.RegisterActivity(activities.ProcessOrder)
  w.RegisterWorkflow(workflows.OrderWorkflow)
  
  w.Run(worker.InterruptCh()) // block forever
}
```

### 2. Kubernetes Environment

The TemporalWorker controller injects these env vars into the Deployment:
- `TEMPORAL_FRONTEND_ADDRESS` = `temporal-frontend.temporal.svc.cluster.local:7233`
- `TEMPORAL_NAMESPACE` = `default` (or from TemporalWorker.spec.namespace)
- `TEMPORAL_TASK_QUEUE` = TemporalWorker.spec.taskQueueName
- Inherited from Pod: `POD_NAME`, `POD_NAMESPACE`, `NODE_NAME` (via `downwardAPI`)

### 3. Temporal Server Expectations

No changes needed. Temporal Frontend auto-discovers task queues as workers connect. A worker that connects to task queue `orders-fifo` will:
- Show up in Temporal UI under `/namespaces/default/task-queues`
- Receive workflows routed to that task queue
- Return activity results to the workflow

### 4. DNS & Network

- Workers must resolve `temporal-frontend.temporal.svc.cluster.local:7233` (CoreDNS, Kubernetes standard)
- Temporal Frontend Service (already exists, type: ClusterIP)
- No ingress needed for worker→Temporal communication (cluster-internal)

## Files to Create/Modify

```
kmsvc-manage/
  api/
    v1/
      temporalworker_types.go          # CRD schema (Phase 1)
  controllers/
    temporalworker_controller.go       # Reconciliation logic (Phase 1)
  config/
    crd/temporal.kmsvc.io_temporalworkers.yaml  # CRD definition YAML

homelab/
  k8s/
    temporal/
      workers/
        worker-orders-fifo.yaml        # Example TemporalWorker instance
        worker-orders-dlq.yaml         # DLQ processor (optional)
      temporal-worker-rbac.yaml        # ServiceAccount + RBAC for worker Deployments
```

## Validation & Testing

1. **Unit tests:** CRD validation, controller reconciliation loops
2. **Integration test (manual):**
   ```bash
   # 1. Deploy Temporal cluster (already done)
   helmfile apply -l name=temporal
   
   # 2. Deploy kmsvc queue-operator with TemporalWorker controller
   helmfile apply -l name=kmsvc-manage  # (future Helm chart)
   
   # 3. Create a TemporalWorker CRD
   kubectl apply -f k8s/temporal/workers/worker-orders-fifo.yaml
   
   # 4. Verify Deployment was created
   kubectl get deploy -n temporal
   kubectl get pods -n temporal -l app=temporal-worker-orders-fifo
   
   # 5. Verify worker is visible in Temporal UI
   curl https://temporal.riotpiao.homelab.com/api/v1/task-queues
   
   # 6. Start a workflow targeting the task queue
   temporal workflow start --task-queue=orders-fifo --type OrderWorkflow
   
   # 7. Verify worker executes the workflow
   kubectl logs -n temporal -f deploy/temporal-worker-orders-fifo
   ```

## Rollout Plan

1. **MVP (kmsvc-manage Phase 1):**
   - Define TemporalWorker CRD + controller
   - Build CRUD reconciliation (create/update/delete Deployments)
   - Document example TemporalWorker manifests
   - User manually creates TemporalWorker CRDs for each queue

2. **Phase 2 (kmsvc-manage Phase 2):**
   - Extend Queue CRD with `temporal.io/worker-enabled` label
   - Auto-generate TemporalWorker on Queue creation
   - User just: `kubectl apply -f queue-orders-fifo.yaml` → worker auto-provisioned

3. **Phase 3 (kmsvc-manage Phase 3):**
   - Hook into Prometheus metrics (queue depth, lag)
   - Scale replicas based on workload
