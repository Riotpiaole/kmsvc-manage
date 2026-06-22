package interceptors

import (
	"context"
	"errors"
	"testing"

	"github.com/lestrrat-go/jwx/v2/jwt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type fakeValidator struct {
	token jwt.Token
	err   error
}

func (f fakeValidator) Validate(ctx context.Context, tokenString string) (jwt.Token, error) {
	return f.token, f.err
}

func contextWithAuthHeader(value string) context.Context {
	if value == "" {
		return context.Background()
	}
	return metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", value))
}

func TestUnaryServerInterceptorRejectsMissingOrMalformedHeader(t *testing.T) {
	validator := fakeValidator{token: jwt.New()}
	interceptor := UnaryServerInterceptor(validator)
	handlerCalled := false
	handler := func(ctx context.Context, req any) (any, error) {
		handlerCalled = true
		return "ok", nil
	}

	tests := []struct {
		name   string
		header string
	}{
		{name: "missing header", header: ""},
		{name: "malformed header (no Bearer prefix)", header: "sometoken"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			handlerCalled = false
			ctx := contextWithAuthHeader(tc.header)
			_, err := interceptor(ctx, nil, &grpc.UnaryServerInfo{}, handler)
			if err == nil {
				t.Fatalf("expected error, got none")
			}
			if status.Code(err) != codes.Unauthenticated {
				t.Fatalf("code = %v, want Unauthenticated", status.Code(err))
			}
			if handlerCalled {
				t.Fatalf("handler must not be called when auth fails")
			}
		})
	}
}

func TestUnaryServerInterceptorRejectsInvalidToken(t *testing.T) {
	validator := fakeValidator{err: errors.New("bad token")}
	interceptor := UnaryServerInterceptor(validator)
	handler := func(ctx context.Context, req any) (any, error) {
		t.Fatalf("handler must not be called when validation fails")
		return nil, nil
	}

	ctx := contextWithAuthHeader("Bearer not-a-real-token")
	_, err := interceptor(ctx, nil, &grpc.UnaryServerInfo{}, handler)
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("code = %v, want Unauthenticated", status.Code(err))
	}
}

func TestUnaryServerInterceptorAllowsValidTokenAndInjectsClaims(t *testing.T) {
	wantToken := jwt.New()
	_ = wantToken.Set(jwt.SubjectKey, "test-subject")
	validator := fakeValidator{token: wantToken}
	interceptor := UnaryServerInterceptor(validator)

	var gotClaims jwt.Token
	handler := func(ctx context.Context, req any) (any, error) {
		claims, ok := ClaimsFromContext(ctx)
		if !ok {
			t.Fatalf("expected claims in context")
		}
		gotClaims = claims
		return "ok", nil
	}

	ctx := contextWithAuthHeader("Bearer a-valid-token")
	resp, err := interceptor(ctx, nil, &grpc.UnaryServerInfo{}, handler)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp != "ok" {
		t.Fatalf("resp = %v, want ok", resp)
	}
	if gotClaims.Subject() != "test-subject" {
		t.Fatalf("subject = %q, want test-subject", gotClaims.Subject())
	}
}
