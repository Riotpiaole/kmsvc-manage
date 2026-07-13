package operator

import "context"

// TemporalNamespaceRegisterer registers a Temporal namespace. reconcileTemporalWorker
// calls this before creating a TemporalWorker so a Queue's temporal.io/namespace
// label always has a real namespace behind it — previously that label was
// trusted as-is, and a typo'd or never-registered namespace would only
// surface as a silently-stuck worker pod polling a namespace that doesn't
// exist.
type TemporalNamespaceRegisterer interface {
	// RegisterNamespace registers namespace, treating AlreadyExists as success.
	RegisterNamespace(ctx context.Context, namespace string) error
}
