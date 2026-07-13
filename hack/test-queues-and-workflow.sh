#!/usr/bin/env bash
set -euo pipefail

# Smoke test for the queue-operator -> TemporalWorker pipeline:
#   1. send/receive/delete a message through each Queue in
#      k8s/temporal/queues/example-queue.yaml (story-crater-tasks,
#      story-crater-notifications) via kmsvc-cli.
#   2. start a helloworld workflow via the temporal CLI against the
#      Temporal namespace those queues point at (temporal.io/namespace
#      label, "production"), on the task queue the auto-created
#      TemporalWorker polls ("worker-production").
#
# Workflow execution will not complete -- there is no real worker image
# registering a "HelloWorldWorkflow" handler yet (TemporalWorker's Deployment
# is running the KMSVC_TEMPORAL_WORKER_IMAGE placeholder). This only proves
# the control-plane path: namespace exists, task queue routing works, and
# StartWorkflowExecution succeeds end-to-end through the operator-managed
# pipeline. `temporal workflow describe` afterward should show it stuck in
# Running/WorkflowTaskScheduled, which is the expected signal that this half
# of the pipe is wired correctly.
#
# Env overrides:
#   NAMESPACE            k8s namespace the Queue CRDs + management-service live in (default: sqs)
#   TEMPORAL_NAMESPACE   Temporal namespace the queues are labeled with (default: production)
#   TASK_QUEUE           Temporal task queue the TemporalWorker polls (default: worker-production)
#   LOCAL_PORT            local port for the management-service port-forward (default: 9090)
#   TEMPORAL_LOCAL_PORT    local port for the temporal-frontend port-forward (default: 7233)
#   AUTHENTIK_TOKEN_URL    Authentik OAuth2 token endpoint

QUEUES=("story-crater-tasks" "story-crater-notifications")
NAMESPACE="${NAMESPACE:-sqs}"
TEMPORAL_NAMESPACE="${TEMPORAL_NAMESPACE:-production}"
TASK_QUEUE="${TASK_QUEUE:-worker-production}"
LOCAL_PORT="${LOCAL_PORT:-9090}"
TEMPORAL_LOCAL_PORT="${TEMPORAL_LOCAL_PORT:-7233}"
SERVER="127.0.0.1:${LOCAL_PORT}"
AUTHENTIK_TOKEN_URL="${AUTHENTIK_TOKEN_URL:-https://authentik.riotpiao.homelab.com/application/o/token/}"

CLIENT_ID=$(talos get cluster/AUTHENTIK_KAFAKA_CLIENT_ID --key AUTHENTIK_KAFAKA_CLIENT_ID)
CLIENT_SECRET=$(talos get cluster/AUTHENTIK_KAFAKA_CLIENT_SECRET --key AUTHENTIK_KAFAKA_CLIENT_SECRET)

TOKEN=$(curl -sf -X POST "${AUTHENTIK_TOKEN_URL}" \
  -d grant_type=client_credentials \
  -d client_id="${CLIENT_ID}" \
  -d client_secret="${CLIENT_SECRET}" \
  | jq -r '.access_token')

if [[ -z "${TOKEN}" || "${TOKEN}" == "null" ]]; then
  echo "FAIL: could not obtain access token from Authentik" >&2
  exit 1
fi

kubectl port-forward -n "${NAMESPACE}" svc/management-service "${LOCAL_PORT}:9090" >/tmp/kmsvc-port-forward.log 2>&1 &
KMSVC_PF_PID=$!
kubectl port-forward -n temporal svc/temporal-frontend "${TEMPORAL_LOCAL_PORT}:7233" >/tmp/temporal-port-forward.log 2>&1 &
TEMPORAL_PF_PID=$!
sleep 2

cleanup() {
  kill "${KMSVC_PF_PID}" "${TEMPORAL_PF_PID}" 2>/dev/null || true
}
trap cleanup EXIT

KMSVC=(kmsvc --server "${SERVER}" --token "${TOKEN}" --insecure)

for QUEUE_NAME in "${QUEUES[@]}"; do
  echo "=== ${QUEUE_NAME} ==="

  PHASE=$(kubectl get queue "${QUEUE_NAME}" -n "${NAMESPACE}" -o jsonpath='{.status.phase}' 2>/dev/null || true)
  if [[ "${PHASE}" != "Ready" ]]; then
    echo "FAIL: queue ${QUEUE_NAME} phase=${PHASE}, want Ready" >&2
    exit 1
  fi

  echo "--- send ---"
  "${KMSVC[@]}" message send --queue "${QUEUE_NAME}" --body "hello from ${QUEUE_NAME}"

  echo "--- receive ---"
  RECEIVE_OUT=$("${KMSVC[@]}" message receive --queue "${QUEUE_NAME}" --max-messages 1 --wait 10 --output json)
  echo "${RECEIVE_OUT}"

  RECEIPT_HANDLE=$(echo "${RECEIVE_OUT}" | jq -r '.[0].receipt_handle // .[0].ReceiptHandle')
  if [[ -z "${RECEIPT_HANDLE}" || "${RECEIPT_HANDLE}" == "null" ]]; then
    echo "FAIL: no message received from ${QUEUE_NAME}" >&2
    exit 1
  fi

  echo "--- delete (ack) ---"
  "${KMSVC[@]}" message delete --queue "${QUEUE_NAME}" --receipt-handle "${RECEIPT_HANDLE}"

  echo "OK: ${QUEUE_NAME} round-trip succeeded"
done

echo "=== temporal: start helloworld workflow ==="
WORKFLOW_ID="hack-helloworld-$(date +%s)"
temporal workflow start \
  --address "127.0.0.1:${TEMPORAL_LOCAL_PORT}" \
  --namespace "${TEMPORAL_NAMESPACE}" \
  --task-queue "${TASK_QUEUE}" \
  --type HelloWorldWorkflow \
  --workflow-id "${WORKFLOW_ID}" \
  --input '"hack smoke test"'

echo "--- describe ---"
temporal workflow describe \
  --address "127.0.0.1:${TEMPORAL_LOCAL_PORT}" \
  --namespace "${TEMPORAL_NAMESPACE}" \
  --workflow-id "${WORKFLOW_ID}"

echo "OK: workflow ${WORKFLOW_ID} started on namespace=${TEMPORAL_NAMESPACE} task-queue=${TASK_QUEUE}"
echo "NOTE: it will not complete -- no worker is registering HelloWorldWorkflow yet (placeholder image on worker-production)."
