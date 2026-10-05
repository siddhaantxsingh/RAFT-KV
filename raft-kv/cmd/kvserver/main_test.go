package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/siddhaantxsingh/raft-kv/kv"
	"github.com/siddhaantxsingh/raft-kv/secure"
	"github.com/siddhaantxsingh/raft-kv/secure/securetest"
)

type testCluster struct {
	urls    []string
	nodes   []*node
	servers []*http.Server
	pki     securetest.PKI
	ctok    string
}

// startSecureCluster runs three replicas in-process on real TLS listeners
// with mTLS, a peer token and a client token.
func startSecureCluster(t *testing.T) *testCluster {
	t.Helper()
	dir := t.TempDir()
	p, err := securetest.Generate(dir)
	if err != nil {
		t.Fatal(err)
	}
	ptok, ctok := "peer-token-0123456789", "client-token-0123456789"
	os.WriteFile(filepath.Join(dir, "peer.tok"), []byte(ptok), 0o600)
	os.WriteFile(filepath.Join(dir, "client.tok"), []byte(ctok), 0o600)

	var lns []net.Listener
	var urls []string
	for i := 0; i < 3; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		lns = append(lns, ln)
		urls = append(urls, "https://"+ln.Addr().String())
	}
	tc := &testCluster{urls: urls, pki: p, ctok: ctok}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	for i := 0; i < 3; i++ {
		c, err := parseFlags([]string{
			"-id", fmt.Sprint(i), "-peers", strings.Join(urls, ","), "-data", dir,
			"-tls-cert", p.NodeCert, "-tls-key", p.NodeKey, "-tls-ca", p.CA,
			"-peer-token-file", filepath.Join(dir, "peer.tok"),
			"-client-token-file", filepath.Join(dir, "client.tok"),
			"-max-value-bytes", "1000",
		})
		if err != nil {
			t.Fatal(err)
		}
		n, err := newNode(c, logger)
		if err != nil {
			t.Fatal(err)
		}
		srv, err := newHTTPServer(c, n.handler)
		if err != nil {
			t.Fatal(err)
		}
		go srv.Serve(tls.NewListener(lns[i], srv.TLSConfig))
		tc.nodes = append(tc.nodes, n)
		tc.servers = append(tc.servers, srv)
	}
	t.Cleanup(func() {
		for i := range tc.nodes {
			tc.servers[i].Close()
			tc.nodes[i].server.Kill()
		}
	})
	return tc
}

func (tc *testCluster) client(t *testing.T, cert, key, token string) *http.Client {
	cfg, err := secure.ClientConfig(secure.TLSFiles{Cert: cert, Key: key, CA: tc.pki.CA})
	if err != nil {
		t.Fatal(err)
	}
	return &http.Client{Timeout: 5 * time.Second, Transport: &secure.TokenTransport{Token: token,
		Base: &http.Transport{TLSClientConfig: cfg}}}
}

func TestSecureClusterEndToEnd(t *testing.T) {
	tc := startSecureCluster(t)
	cfg, _ := secure.ClientConfig(secure.TLSFiles{Cert: tc.pki.ClientCert, Key: tc.pki.ClientKey, CA: tc.pki.CA})
	ck := kv.NewClerk(kv.NewSecureHTTPCaller(tc.urls, cfg, tc.ctok))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := ck.Put(ctx, "k", "v1"); err != nil {
		t.Fatalf("put over mTLS: %v", err)
	}
	if v, _, err := ck.Get(ctx, "k"); err != nil || v != "v1" {
		t.Fatalf("get: %q %v", v, err)
	}

	leader := -1
	for i, n := range tc.nodes {
		if n.server.Raft().Status().Role == "leader" {
			leader = i
		}
	}
	if leader < 0 {
		t.Fatal("no leader")
	}
	lurl := tc.urls[leader]
	status := func(c *http.Client, method, url, body string) int {
		req, _ := http.NewRequest(method, url, strings.NewReader(body))
		resp, err := c.Do(req)
		if err != nil {
			return -1
		}
		resp.Body.Close()
		return resp.StatusCode
	}

	// Client token required on /kv; peer token required on /raft.
	noTok := tc.client(t, tc.pki.ClientCert, tc.pki.ClientKey, "")
	if c := status(noTok, "GET", lurl+"/kv/k", ""); c != 401 {
		t.Errorf("/kv without token: %d", c)
	}
	withClientTok := tc.client(t, tc.pki.ClientCert, tc.pki.ClientKey, tc.ctok)
	if c := status(withClientTok, "POST", lurl+"/raft/vote", "x"); c != 401 {
		t.Errorf("/raft with the client token: %d", c)
	}
	// Certificates from another CA are refused at the TLS layer.
	rogue := tc.client(t, tc.pki.RogueCert, tc.pki.RogueKey, tc.ctok)
	if c := status(rogue, "GET", lurl+"/kv/k", ""); c != -1 {
		t.Errorf("rogue certificate got HTTP %d", c)
	}
	// Oversized values are rejected, not truncated.
	if c := status(withClientTok, "PUT", lurl+"/kv/big", strings.Repeat("x", 1001)); c != 413 {
		t.Errorf("oversized PUT: %d", c)
	}
	if _, found, _ := ck.Get(ctx, "big"); found {
		t.Error("oversized value was stored")
	}
	// Operational endpoints stay reachable for probes (mTLS still applies).
	if c := status(noTok, "GET", lurl+"/readyz", ""); c != 200 {
		t.Errorf("/readyz on leader: %d", c)
	}
	if c := status(noTok, "GET", lurl+"/metrics", ""); c != 200 {
		t.Errorf("/metrics: %d", c)
	}
}

func TestParseFlagsRejectsSchemeMismatch(t *testing.T) {
	if _, err := parseFlags([]string{"-peers", "http://a:1", "-tls-cert", "c", "-tls-key", "k"}); err == nil {
		t.Error("TLS with http:// peers accepted")
	}
	if _, err := parseFlags([]string{"-peers", "https://a:1"}); err == nil {
		t.Error("https:// peers without TLS accepted")
	}
	if _, err := parseFlags([]string{"-id", "3", "-peers", "http://a:1"}); err == nil {
		t.Error("out-of-range id accepted")
	}
}
