#!/usr/bin/env bash
set -euo pipefail

# Round-trip smoke test: create a queue (via CRD), send + receive + delete a
# message through kmsvc-cli, then tear the queue down.
#
# Connects via a kubectl port-forward to the management-service ClusterIP
# (bypasses ingress/DNS) and authenticates with an Authentik client_credentials
# token, since every RPC requires a real bearer token -- see
# internal/api/interceptors/auth.go.
#
# Env overrides:
#   NAMESPACE         k8s namespace the Queue CRD + service live in (default: sqs)
#   LOCAL_PORT        local port for the port-forward (default: 9090)
#   AUTHENTIK_TOKEN_URL  Authentik OAuth2 token endpoint
#                        (default: https://authentik.riotpiao.homelab.com/application/o/token/)

QUEUE_NAME="${QUEUE_NAME:-smoke-test-queue}"
NAMESPACE="${NAMESPACE:-sqs}"
LOCAL_PORT="${LOCAL_PORT:-9090}"
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
PORT_FORWARD_PID=$!
sleep 2

cleanup() {
  echo "--- deleteQueue (kubectl delete) ---"
  kubectl delete queue "${QUEUE_NAME}" -n "${NAMESPACE}" --ignore-not-found
  kill "${PORT_FORWARD_PID}" 2>/dev/null || true
}
trap cleanup EXIT

echo "--- createQueue (kubectl apply) ---"
cat <<EOF | kubectl apply -f -
apiVersion: kmsvc.io/v1
kind: Queue
metadata:
  name: ${QUEUE_NAME}
  namespace: ${NAMESPACE}
spec:
  visibilityTimeoutSeconds: 30
  messageRetentionPeriodSeconds: 345600
  maxReceiveCount: 5
  partitionsPerShard: 1
  minShards: 1
  maxShards: 1
EOF

# Queue CRD only sets status.phase (Pending|Ready|Failed), no
# status.conditions[] -- so "kubectl wait --for=condition=Ready" never
# matches. Poll the phase field directly instead.
echo "--- waiting for queue phase=Ready ---"
for i in $(seq 1 30); do
  PHASE=$(kubectl get queue "${QUEUE_NAME}" -n "${NAMESPACE}" -o jsonpath='{.status.phase}' 2>/dev/null || true)
  if [[ "${PHASE}" == "Ready" ]]; then
    break
  fi
  if [[ "${PHASE}" == "Failed" ]]; then
    echo "FAIL: queue reconcile failed" >&2
    kubectl get queue "${QUEUE_NAME}" -n "${NAMESPACE}" -o yaml >&2
    exit 1
  fi
  sleep 2
done
if [[ "${PHASE}" != "Ready" ]]; then
  echo "FAIL: queue never reached Ready within 60s (phase=${PHASE})" >&2
  kubectl get queue "${QUEUE_NAME}" -n "${NAMESPACE}" -o yaml >&2
  exit 1
fi

KMSVC=(kmsvc --server "${SERVER}" --token "${TOKEN}" --insecure)

echo "--- send ---"
SEND_OUT=$("${KMSVC[@]}" message send --queue "${QUEUE_NAME}" --body "hello from smoke test")
echo "${SEND_OUT}"

echo "--- receive ---"
RECEIVE_OUT=$("${KMSVC[@]}" message receive --queue "${QUEUE_NAME}" --max-messages 1 --wait 10 --output json)
echo "${RECEIVE_OUT}"

RECEIPT_HANDLE=$(echo "${RECEIVE_OUT}" | jq -r '.[0].receipt_handle // .[0].ReceiptHandle')
if [[ -z "${RECEIPT_HANDLE}" || "${RECEIPT_HANDLE}" == "null" ]]; then
  echo "FAIL: no message received" >&2
  exit 1
fi

echo "--- delete (ack) ---"
"${KMSVC[@]}" message delete --queue "${QUEUE_NAME}" --receipt-handle "${RECEIPT_HANDLE}"

echo "OK: send/receive/delete round-trip succeeded"
