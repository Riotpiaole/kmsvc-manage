// Package auth implements §8 of design.md: Authentik OIDC discovery + JWKS
// caching, and JWT validation shared by the gRPC and REST (grpc-gateway)
// transports via a single interceptor.
package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/lestrrat-go/jwx/v2/jwk"
)

// DiscoveryDocument is the subset of an OIDC discovery document this package
// needs.
type DiscoveryDocument struct {
	Issuer  string `json:"issuer"`
	JWKSURI string `json:"jwks_uri"`
}

// FetchDiscoveryDocument GETs issuerURL's well-known OIDC discovery document.
func FetchDiscoveryDocument(ctx context.Context, issuerURL string) (*DiscoveryDocument, error) {
	url := strings.TrimRight(issuerURL, "/") + "/.well-known/openid-configuration"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build discovery request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch discovery document %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch discovery document %s: status %d", url, resp.StatusCode)
	}

	var doc DiscoveryDocument
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		return nil, fmt.Errorf("decode discovery document %s: %w", url, err)
	}
	return &doc, nil
}

// KeySetProvider returns the current JWKS for validating a token's
// signature. Abstracted so tests can inject a static key set instead of
// running a real JWKS HTTP endpoint.
type KeySetProvider interface {
	Get(ctx context.Context) (jwk.Set, error)
}

// JWKSCache fetches a JWKS URI once at startup and keeps it fresh via a
// background auto-refresh, so request-path validation never makes a network
// call (design.md §8).
type JWKSCache struct {
	jwksURI string
	cache   *jwk.Cache
}

// NewJWKSCache registers jwksURI with a background auto-refreshing cache and
// performs the initial fetch synchronously so startup fails fast on a
// misconfigured/unreachable JWKS endpoint.
func NewJWKSCache(ctx context.Context, jwksURI string) (*JWKSCache, error) {
	cache := jwk.NewCache(ctx)
	if err := cache.Register(jwksURI, jwk.WithMinRefreshInterval(15*time.Minute)); err != nil {
		return nil, fmt.Errorf("register jwks cache %s: %w", jwksURI, err)
	}
	if _, err := cache.Refresh(ctx, jwksURI); err != nil {
		return nil, fmt.Errorf("initial jwks fetch %s: %w", jwksURI, err)
	}
	return &JWKSCache{jwksURI: jwksURI, cache: cache}, nil
}

// Get returns the cached key set, refreshing in the background per the
// interval configured in NewJWKSCache rather than on every call.
func (c *JWKSCache) Get(ctx context.Context) (jwk.Set, error) {
	return c.cache.Get(ctx, c.jwksURI)
}

// staticKeySet is a KeySetProvider backed by a fixed jwk.Set, used in tests
// to avoid standing up a real JWKS HTTP endpoint.
type staticKeySet struct {
	set jwk.Set
}

func StaticKeySet(set jwk.Set) KeySetProvider {
	return staticKeySet{set: set}
}

func (s staticKeySet) Get(ctx context.Context) (jwk.Set, error) {
	return s.set, nil
}
