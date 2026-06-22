// Package interceptors holds gRPC interceptors shared by both transports:
// grpc-gateway forwards the incoming REST request's Authorization header as
// gRPC metadata, so the same unary/stream interceptor authenticates both
// REST and gRPC callers (design.md §8) — no separate REST auth middleware.
package interceptors

import (
	"context"
	"strings"

	"github.com/lestrrat-go/jwx/v2/jwt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type claimsContextKey struct{}

// ClaimsFromContext returns the authenticated token's claims, set by the
// auth interceptor after a successful validation.
func ClaimsFromContext(ctx context.Context) (jwt.Token, bool) {
	token, ok := ctx.Value(claimsContextKey{}).(jwt.Token)
	return token, ok
}

// TokenValidator is the subset of auth.Validator the interceptor needs,
// abstracted so tests can inject a fake without a real JWKS endpoint.
type TokenValidator interface {
	Validate(ctx context.Context, tokenString string) (jwt.Token, error)
}

func bearerToken(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", status.Error(codes.Unauthenticated, "missing metadata")
	}
	values := md.Get("authorization")
	if len(values) == 0 {
		return "", status.Error(codes.Unauthenticated, "missing authorization header")
	}
	const prefix = "Bearer "
	header := values[0]
	if !strings.HasPrefix(header, prefix) {
		return "", status.Error(codes.Unauthenticated, "authorization header must be a bearer token")
	}
	return strings.TrimPrefix(header, prefix), nil
}

// UnaryServerInterceptor authenticates every unary RPC before it reaches
// handler logic.
func UnaryServerInterceptor(validator TokenValidator) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		token, err := bearerToken(ctx)
		if err != nil {
			return nil, err
		}
		claims, err := validator.Validate(ctx, token)
		if err != nil {
			return nil, status.Error(codes.Unauthenticated, "invalid token")
		}
		return handler(context.WithValue(ctx, claimsContextKey{}, claims), req)
	}
}

// StreamServerInterceptor authenticates every streaming RPC before it
// reaches handler logic.
func StreamServerInterceptor(validator TokenValidator) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		token, err := bearerToken(ss.Context())
		if err != nil {
			return err
		}
		claims, err := validator.Validate(ss.Context(), token)
		if err != nil {
			return status.Error(codes.Unauthenticated, "invalid token")
		}
		return handler(srv, &authenticatedStream{ServerStream: ss, ctx: context.WithValue(ss.Context(), claimsContextKey{}, claims)})
	}
}

type authenticatedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *authenticatedStream) Context() context.Context {
	return s.ctx
}
