package secure

import (
	"crypto/tls"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/siddhaantxsingh/raft-kv/secure/securetest"
)

type pki struct{ ca, srvCert, srvKey, cliCert, cliKey, rogueCert, rogueKey string }

func newPKI(t *testing.T) pki {
	g, err := securetest.Generate(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return pki{g.CA, g.NodeCert, g.NodeKey, g.ClientCert, g.ClientKey, g.RogueCert, g.RogueKey}
}

func tlsServer(t *testing.T, cfg *tls.Config, h http.Handler) *httptest.Server {
	ts := httptest.NewUnstartedServer(h)
	ts.TLS = cfg
	ts.StartTLS()
	t.Cleanup(ts.Close)
	return ts
}

func get(c *tls.Config, url string, hdr map[string]string) (int, error) {
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := (&http.Client{Transport: &http.Transport{TLSClientConfig: c}, Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		return 0, err
	}
	resp.Body.Close()
	return resp.StatusCode, nil
}

var ok = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) })

func TestMutualTLS(t *testing.T) {
	p := newPKI(t)
	srvCfg, err := ServerConfig(TLSFiles{Cert: p.srvCert, Key: p.srvKey, CA: p.ca})
	if err != nil {
		t.Fatal(err)
	}
	ts := tlsServer(t, srvCfg, ok)

	good, _ := ClientConfig(TLSFiles{Cert: p.cliCert, Key: p.cliKey, CA: p.ca})
	if code, err := get(good, ts.URL, nil); err != nil || code != 200 {
		t.Fatalf("client with CA-signed cert: %d %v", code, err)
	}
	noCert, _ := ClientConfig(TLSFiles{CA: p.ca})
	if _, err := get(noCert, ts.URL, nil); err == nil {
		t.Fatal("client without certificate was accepted")
	}
	rogue, _ := ClientConfig(TLSFiles{Cert: p.rogueCert, Key: p.rogueKey, CA: p.ca})
	if _, err := get(rogue, ts.URL, nil); err == nil {
		t.Fatal("client with a certificate from another CA was accepted")
	}
	// The client refuses a server its CA did not sign.
	otherSrv := httptest.NewTLSServer(ok) // self-signed test cert
	defer otherSrv.Close()
	if _, err := get(good, otherSrv.URL, nil); err == nil {
		t.Fatal("client accepted a server outside the cluster CA")
	}
}

func TestServerOnlyTLSAndBadFiles(t *testing.T) {
	p := newPKI(t)
	srvCfg, err := ServerConfig(TLSFiles{Cert: p.srvCert, Key: p.srvKey})
	if err != nil {
		t.Fatal(err)
	}
	ts := tlsServer(t, srvCfg, ok)
	c, _ := ClientConfig(TLSFiles{CA: p.ca})
	if code, err := get(c, ts.URL, nil); err != nil || code != 200 {
		t.Fatalf("server-only TLS: %d %v", code, err)
	}
	if _, err := ServerConfig(TLSFiles{Cert: p.srvCert}); err == nil {
		t.Fatal("missing key accepted")
	}
	if _, err := ClientConfig(TLSFiles{CA: p.srvKey}); err == nil {
		t.Fatal("non-certificate CA file accepted")
	}
}

func TestRequireToken(t *testing.T) {
	ts := httptest.NewServer(RequireToken("0123456789abcdef-secret", ok))
	defer ts.Close()
	for hdr, want := range map[string]int{
		"":                               401,
		"Bearer wrong":                   401,
		"bearer 0123456789abcdef-secret": 401,
		"Bearer 0123456789abcdef-secret": 200,
	} {
		h := map[string]string{}
		if hdr != "" {
			h["Authorization"] = hdr
		}
		if code, _ := get(nil, ts.URL, h); code != want {
			t.Errorf("Authorization %q: status %d, want %d", hdr, code, want)
		}
	}
	// TokenTransport adds the header.
	c := &http.Client{Transport: &TokenTransport{Token: "0123456789abcdef-secret"}}
	resp, err := c.Get(ts.URL)
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("TokenTransport: %v %v", resp, err)
	}
	resp.Body.Close()
}

func TestReadTokenRejectsShortTokens(t *testing.T) {
	p := filepath.Join(t.TempDir(), "tok")
	os.WriteFile(p, []byte("short\n"), 0o600)
	if _, err := ReadToken(p); err == nil {
		t.Fatal("short token accepted")
	}
	os.WriteFile(p, []byte("a-sufficiently-long-token\n"), 0o600)
	if tok, err := ReadToken(p); err != nil || tok != "a-sufficiently-long-token" {
		t.Fatalf("got %q %v", tok, err)
	}
}

func TestLimitBody(t *testing.T) {
	var gotErr error
	ts := httptest.NewServer(LimitBody(10, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, gotErr = io.ReadAll(r.Body)
	})))
	defer ts.Close()
	http.Post(ts.URL, "text/plain", strings.NewReader(strings.Repeat("x", 100)))
	if !IsTooLarge(gotErr) {
		t.Fatalf("want MaxBytesError, got %v", gotErr)
	}
}
