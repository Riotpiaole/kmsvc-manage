// Package temporal wraps the narrow slice of Temporal's WorkflowService that
// queue-operator needs (namespace registration) directly over gRPC, instead
// of pulling in the full Temporal Go SDK for one RPC.
package temporal

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

// defaultRetentionPeriod is applied to namespaces queue-operator registers on
// a Queue's behalf. Namespaces created deliberately by an operator (e.g. via
// the temporal CLI) can still override this by re-registering with different
// settings — RegisterNamespace on an existing namespace is a no-op here.
const defaultRetentionPeriod = 72 * time.Hour

// Client wraps a Temporal frontend's WorkflowService.
type Client struct {
	svc workflowservice.WorkflowServiceClient
}

// NewClient dials a Temporal frontend at address (e.g.
// "temporal-frontend.temporal.svc.cluster.local:7233"). The connection is
// plaintext, matching how TemporalWorkerReconciler's worker pods already
// talk to the same frontend (see temporal_worker_controller.go).
func NewClient(address string) (*Client, error) {
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial temporal frontend %s: %w", address, err)
	}
	return &Client{svc: workflowservice.NewWorkflowServiceClient(conn)}, nil
}

// RegisterNamespace registers namespace, treating AlreadyExists as success.
func (c *Client) RegisterNamespace(ctx context.Context, namespace string) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	_, err := c.svc.RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		WorkflowExecutionRetentionPeriod: durationpb.New(defaultRetentionPeriod),
	})
	if err != nil && status.Code(err) != codes.AlreadyExists {
		return fmt.Errorf("register temporal namespace %s: %w", namespace, err)
	}
	return nil
}
