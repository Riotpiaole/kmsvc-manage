package operator

import (
	"context"
	"fmt"
	"sort"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// zoneLabel is the standard k8s topology label this cluster's Talos nodes
// carry (confirmed via `kubectl get nodes --show-labels`).
const zoneLabel = "topology.kubernetes.io/zone"

// ZoneLocator resolves which availability zones (node labels) a set of
// Kafka broker IDs currently run in, for AZ-aware Queue status (design.md
// §2a). Broker pods follow Strimzi's StrimziPodSet naming convention:
// "<clusterName>-<poolName>-<brokerID>" (confirmed live: broker id 0 is pod
// kmsvc-kmsvc-pool-0, etc. -- broker ID equals the pod's ordinal suffix).
type ZoneLocator struct {
	// Client only needs Get, so callers can pass an uncached client.Reader
	// (e.g. mgr.GetAPIReader()) and avoid requiring cluster-wide list/watch
	// RBAC on Pods/Nodes just to serve occasional point lookups here.
	Client      client.Reader
	Namespace   string
	ClusterName string
	PoolName    string

	// nodeZoneCache avoids a Node lookup per broker per reconcile -- node
	// zone labels don't change while a node is up, and this is best-effort
	// (cleared implicitly by process restart) rather than watched for
	// correctness.
	nodeZoneCache map[string]string
}

// ZonesForBrokers returns the sorted, de-duplicated set of zones the given
// broker IDs currently run in. A broker whose pod or node can't be resolved
// is skipped rather than failing the whole call -- AZ info is best-effort
// status, not load-bearing for reconciliation.
func (z *ZoneLocator) ZonesForBrokers(ctx context.Context, brokerIDs []int32) ([]string, error) {
	if z.nodeZoneCache == nil {
		z.nodeZoneCache = make(map[string]string)
	}

	seen := make(map[string]bool)
	var zones []string
	for _, id := range brokerIDs {
		nodeName, err := z.nodeNameForBroker(ctx, id)
		if err != nil || nodeName == "" {
			continue
		}
		zone, err := z.zoneForNode(ctx, nodeName)
		if err != nil || zone == "" {
			continue
		}
		if !seen[zone] {
			seen[zone] = true
			zones = append(zones, zone)
		}
	}
	sort.Strings(zones)
	return zones, nil
}

func (z *ZoneLocator) nodeNameForBroker(ctx context.Context, brokerID int32) (string, error) {
	podName := fmt.Sprintf("%s-%s-%d", z.ClusterName, z.PoolName, brokerID)
	var pod corev1.Pod
	if err := z.Client.Get(ctx, client.ObjectKey{Namespace: z.Namespace, Name: podName}, &pod); err != nil {
		if apierrors.IsNotFound(err) {
			return "", nil
		}
		return "", fmt.Errorf("get broker pod %s: %w", podName, err)
	}
	return pod.Spec.NodeName, nil
}

func (z *ZoneLocator) zoneForNode(ctx context.Context, nodeName string) (string, error) {
	if zone, ok := z.nodeZoneCache[nodeName]; ok {
		return zone, nil
	}
	var node corev1.Node
	if err := z.Client.Get(ctx, client.ObjectKey{Name: nodeName}, &node); err != nil {
		if apierrors.IsNotFound(err) {
			return "", nil
		}
		return "", fmt.Errorf("get node %s: %w", nodeName, err)
	}
	zone := node.Labels[zoneLabel]
	z.nodeZoneCache[nodeName] = zone
	return zone, nil
}
