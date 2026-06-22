package auth

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"testing"
	"time"

	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/lestrrat-go/jwx/v2/jwk"
	"github.com/lestrrat-go/jwx/v2/jwt"
)

const (
	testIssuer   = "https://authentik.homelab.internal/application/o/kmsvc/"
	testAudience = "kmsvc"
	testKeyID    = "test-key-1"
)

func generateRSAKeyPair(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate rsa key: %v", err)
	}
	return key
}

func publicKeySet(t *testing.T, priv *rsa.PrivateKey, kid string) jwk.Set {
	t.Helper()
	pubKey, err := jwk.FromRaw(priv.PublicKey)
	if err != nil {
		t.Fatalf("jwk from raw: %v", err)
	}
	if err := pubKey.Set(jwk.KeyIDKey, kid); err != nil {
		t.Fatalf("set kid: %v", err)
	}
	if err := pubKey.Set(jwk.AlgorithmKey, jwa.RS256); err != nil {
		t.Fatalf("set alg: %v", err)
	}
	set := jwk.NewSet()
	if err := set.AddKey(pubKey); err != nil {
		t.Fatalf("add key to set: %v", err)
	}
	return set
}

// signToken builds and signs a JWT with the given claims overrides, using
// sane defaults (valid iss/aud/exp) so each test case only needs to override
// what it's testing.
func signToken(t *testing.T, priv *rsa.PrivateKey, kid string, opts ...func(*jwt.Builder)) string {
	t.Helper()
	builder := jwt.NewBuilder().
		Issuer(testIssuer).
		Audience([]string{testAudience}).
		IssuedAt(time.Now()).
		Expiration(time.Now().Add(time.Hour))
	for _, opt := range opts {
		opt(builder)
	}
	token, err := builder.Build()
	if err != nil {
		t.Fatalf("build token: %v", err)
	}
	signingKey, err := jwk.FromRaw(priv)
	if err != nil {
		t.Fatalf("jwk from raw priv: %v", err)
	}
	if err := signingKey.Set(jwk.KeyIDKey, kid); err != nil {
		t.Fatalf("set signing kid: %v", err)
	}
	signed, err := jwt.Sign(token, jwt.WithKey(jwa.RS256, signingKey))
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}
	return string(signed)
}

func TestValidatorTableDriven(t *testing.T) {
	priv := generateRSAKeyPair(t)
	otherPriv := generateRSAKeyPair(t)
	keySet := publicKeySet(t, priv, testKeyID)

	validator := &Validator{Issuer: testIssuer, Audience: testAudience, Keys: StaticKeySet(keySet)}

	tests := []struct {
		name      string
		token     string
		wantError bool
	}{
		{
			name:  "valid token",
			token: signToken(t, priv, testKeyID),
		},
		{
			name: "expired token",
			token: signToken(t, priv, testKeyID, func(b *jwt.Builder) {
				b.Expiration(time.Now().Add(-time.Hour))
			}),
			wantError: true,
		},
		{
			name: "wrong issuer",
			token: signToken(t, priv, testKeyID, func(b *jwt.Builder) {
				b.Issuer("https://not-authentik.example.com/")
			}),
			wantError: true,
		},
		{
			name: "wrong audience",
			token: signToken(t, priv, testKeyID, func(b *jwt.Builder) {
				b.Audience([]string{"some-other-service"})
			}),
			wantError: true,
		},
		{
			name:      "bad signature (signed by a different key)",
			token:     signToken(t, otherPriv, testKeyID),
			wantError: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := validator.Validate(context.Background(), tc.token)
			if tc.wantError && err == nil {
				t.Fatalf("expected error, got none")
			}
			if !tc.wantError && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}
