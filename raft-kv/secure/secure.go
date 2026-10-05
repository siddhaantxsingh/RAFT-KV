// Package secure holds the transport-security pieces shared by the server
// and clients: TLS/mTLS configuration from PEM files, bearer-token
// authentication, and request body limits.
//
// Threat model (see docs/SECURITY.md): without TLS, anyone who can reach a
// replica's port can read and forge Raft RPCs and client requests. With
// mTLS, peers and clients must present a certificate signed by the
// cluster CA. Bearer tokens are an independent (or additional) check for
// deployments that terminate TLS elsewhere.
package secure

import (
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
)

// TLSFiles names PEM files. CA is optional for a server (no client
// verification) but enables mutual TLS when set.
type TLSFiles struct {
	Cert, Key, CA string
}

func (f TLSFiles) Enabled() bool { return f.Cert != "" || f.Key != "" || f.CA != "" }

func loadPool(path string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("%s: no PEM certificates", path)
	}
	return pool, nil
}

// ServerConfig builds a server TLS config. With f.CA set, clients must
// present a certificate signed by that CA (mutual TLS).
func ServerConfig(f TLSFiles) (*tls.Config, error) {
	if f.Cert == "" || f.Key == "" {
		return nil, errors.New("tls: both certificate and key are required")
	}
	cert, err := tls.LoadX509KeyPair(f.Cert, f.Key)
	if err != nil {
		return nil, err
	}
	cfg := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	if f.CA != "" {
		pool, err := loadPool(f.CA)
		if err != nil {
			return nil, err
		}
		cfg.ClientCAs = pool
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return cfg, nil
}

// ClientConfig builds a client TLS config that verifies servers against
// f.CA (system roots if empty) and presents f.Cert/f.Key if set (mTLS).
func ClientConfig(f TLSFiles) (*tls.Config, error) {
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if f.CA != "" {
		pool, err := loadPool(f.CA)
		if err != nil {
			return nil, err
		}
		cfg.RootCAs = pool
	}
	if f.Cert != "" || f.Key != "" {
		cert, err := tls.LoadX509KeyPair(f.Cert, f.Key)
		if err != nil {
			return nil, err
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}

// ReadToken reads a bearer token from a file (trailing whitespace
// trimmed). An empty path means "no token".
func ReadToken(path string) (string, error) {
	if path == "" {
		return "", nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	t := strings.TrimSpace(string(b))
	if len(t) < 16 {
		return "", fmt.Errorf("%s: token must be at least 16 characters", path)
	}
	return t, nil
}

// RequireToken rejects requests without "Authorization: Bearer <token>"
// (constant-time comparison). An empty token disables the check.
func RequireToken(token string, next http.Handler) http.Handler {
	if token == "" {
		return next
	}
	want := []byte("Bearer " + token)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got := []byte(r.Header.Get("Authorization"))
		if subtle.ConstantTimeCompare(got, want) != 1 {
			w.Header().Set("WWW-Authenticate", `Bearer realm="raft-kv"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// LimitBody caps request bodies at n bytes; reading past it fails and the
// handler should answer 413 (see IsTooLarge).
func LimitBody(n int64, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, n)
		next.ServeHTTP(w, r)
	})
}

// IsTooLarge reports whether err came from a body over its LimitBody cap.
func IsTooLarge(err error) bool {
	var mbe *http.MaxBytesError
	return errors.As(err, &mbe)
}

// TokenTransport adds a bearer token to every request.
type TokenTransport struct {
	Token string
	Base  http.RoundTripper
}

func (t *TokenTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	base := t.Base
	if base == nil {
		base = http.DefaultTransport
	}
	if t.Token == "" {
		return base.RoundTrip(r)
	}
	r = r.Clone(r.Context())
	r.Header.Set("Authorization", "Bearer "+t.Token)
	return base.RoundTrip(r)
}
