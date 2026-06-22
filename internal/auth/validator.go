package auth

import (
	"context"
	"fmt"

	"github.com/lestrrat-go/jwx/v2/jwt"
)

// Validator validates a bearer JWT's signature, issuer, audience, and
// exp/nbf claims against a cached JWKS (design.md §8).
type Validator struct {
	Issuer   string
	Audience string
	Keys     KeySetProvider
}

// NewValidator does the OIDC discovery + JWKS cache setup and returns a
// ready-to-use Validator. Fails fast if the discovery document or initial
// JWKS fetch is unreachable.
func NewValidator(ctx context.Context, issuerURL, audience string) (*Validator, error) {
	doc, err := FetchDiscoveryDocument(ctx, issuerURL)
	if err != nil {
		return nil, fmt.Errorf("new validator: %w", err)
	}
	cache, err := NewJWKSCache(ctx, doc.JWKSURI)
	if err != nil {
		return nil, fmt.Errorf("new validator: %w", err)
	}
	return &Validator{Issuer: doc.Issuer, Audience: audience, Keys: cache}, nil
}

// Validate parses and verifies tokenString: signature against the current
// JWKS, plus iss/aud/exp/nbf claims. Returns the parsed token on success.
func (v *Validator) Validate(ctx context.Context, tokenString string) (jwt.Token, error) {
	set, err := v.Keys.Get(ctx)
	if err != nil {
		return nil, fmt.Errorf("validate: fetch key set: %w", err)
	}

	token, err := jwt.Parse(
		[]byte(tokenString),
		jwt.WithKeySet(set),
		jwt.WithValidate(true),
		jwt.WithIssuer(v.Issuer),
		jwt.WithAudience(v.Audience),
	)
	if err != nil {
		return nil, fmt.Errorf("validate: %w", err)
	}
	return token, nil
}
