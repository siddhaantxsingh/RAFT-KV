// Command kvserver runs one replica of the Raft-backed key/value store.
//
//	kvserver -id 0 -peers http://n0:7000,http://n1:7000,http://n2:7000 -data /var/lib/kv
//
// Each replica serves peer RPCs (/raft/*), the client API (/kv/*) and the
// operational endpoints (/status, /metrics, /healthz, /readyz) on one
// address. With -tls-cert/-tls-key the address speaks TLS (peer URLs must
// be https://); adding -tls-ca requires every peer and client to present a
// certificate signed by that CA (mutual TLS). -peer-token-file and
// -client-token-file add bearer-token checks on /raft/* and /kv/*.
// Operational endpoints are not authenticated; they expose no data.
package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/siddhaantxsingh/raft-kv/kv"
	"github.com/siddhaantxsingh/raft-kv/raft"
	"github.com/siddhaantxsingh/raft-kv/secure"
)

type config struct {
	id              int
	peers           []string
	dataDir         string
	listen          string
	maxState        int
	verbose         bool
	logReads        bool
	logFormat       string
	tls             secure.TLSFiles
	peerTokenFile   string
	clientTokenFile string
	maxKeyBytes     int
	maxValueBytes   int64
	maxSessions     int
	readyMaxLag     int
	store           string
}

func parseFlags(args []string) (config, error) {
	var c config
	fs := flag.NewFlagSet("kvserver", flag.ContinueOnError)
	fs.IntVar(&c.id, "id", 0, "this replica's index into -peers")
	peers := fs.String("peers", "http://127.0.0.1:7000", "comma-separated peer URLs (same order on every replica)")
	fs.StringVar(&c.dataDir, "data", "./data", "directory for Raft state and snapshots")
	fs.StringVar(&c.listen, "listen", "", "listen address (default: :port of own peer URL)")
	fs.IntVar(&c.maxState, "snapshot-bytes", 4<<20, "snapshot when Raft state exceeds this many bytes (-1 disables)")
	fs.BoolVar(&c.verbose, "v", false, "log Raft state transitions")
	fs.BoolVar(&c.logReads, "log-reads", false, "route Gets through the Raft log instead of ReadIndex")
	fs.StringVar(&c.logFormat, "log-format", "text", "log format: text or json")
	fs.StringVar(&c.tls.Cert, "tls-cert", "", "PEM certificate (enables TLS)")
	fs.StringVar(&c.tls.Key, "tls-key", "", "PEM private key")
	fs.StringVar(&c.tls.CA, "tls-ca", "", "PEM CA bundle: verify peers, and require client certificates (mTLS)")
	fs.StringVar(&c.peerTokenFile, "peer-token-file", "", "file holding the bearer token required on /raft/* (and sent to peers)")
	fs.StringVar(&c.clientTokenFile, "client-token-file", "", "file holding the bearer token required on /kv/*")
	fs.IntVar(&c.maxKeyBytes, "max-key-bytes", kv.DefaultMaxKeyBytes, "largest accepted key")
	fs.Int64Var(&c.maxValueBytes, "max-value-bytes", kv.DefaultMaxValueBytes, "largest accepted value")
	fs.IntVar(&c.maxSessions, "max-sessions", kv.DefaultMaxSessions, "client session table bound (must match on all replicas)")
	fs.IntVar(&c.readyMaxLag, "ready-max-lag", 1000, "/readyz fails while more than this many committed entries are unapplied")
	fs.StringVar(&c.store, "store", "memory", "state machine store: memory, or lsm (binary built with -tags lsm)")
	if err := fs.Parse(args); err != nil {
		return c, err
	}
	c.peers = strings.Split(*peers, ",")
	if c.id < 0 || c.id >= len(c.peers) {
		return c, fmt.Errorf("-id %d out of range for %d peers", c.id, len(c.peers))
	}
	for _, p := range c.peers {
		u, err := url.Parse(p)
		if err != nil || u.Host == "" {
			return c, fmt.Errorf("bad peer URL %q", p)
		}
		if c.tls.Enabled() && u.Scheme != "https" {
			return c, fmt.Errorf("TLS is enabled but peer URL %q is not https://", p)
		}
		if !c.tls.Enabled() && u.Scheme != "http" {
			return c, fmt.Errorf("peer URL %q is https:// but no -tls-cert/-tls-key given", p)
		}
	}
	if c.listen == "" {
		u, _ := url.Parse(c.peers[c.id])
		c.listen = ":" + u.Port()
	}
	if c.store != "memory" && c.store != "lsm" {
		return c, fmt.Errorf("-store must be memory or lsm")
	}
	if c.logFormat != "text" && c.logFormat != "json" {
		return c, fmt.Errorf("-log-format must be text or json")
	}
	return c, nil
}

// node is one running replica: the KV server plus its HTTP handler.
type node struct {
	cfg     config
	server  *kv.Server
	storage raft.Storage
	handler http.Handler
	metrics *requestMetrics
	raftCfg raft.Config
}

func newNode(c config, logger *slog.Logger) (*node, error) {
	peerToken, err := secure.ReadToken(c.peerTokenFile)
	if err != nil {
		return nil, err
	}
	clientToken, err := secure.ReadToken(c.clientTokenFile)
	if err != nil {
		return nil, err
	}
	peerTLS, err := peerTLSConfig(c.tls)
	if err != nil {
		return nil, err
	}

	storage, err := raft.NewFileStorage(fmt.Sprintf("%s/node-%d", c.dataDir, c.id))
	if err != nil {
		return nil, err
	}
	rcfg := raft.DefaultConfig()
	if c.verbose {
		rcfg.Logger = slog.NewLogLogger(logger.Handler(), slog.LevelInfo)
	}

	n := &node{cfg: c, storage: storage, metrics: newRequestMetrics(), raftCfg: rcfg}
	mux := http.NewServeMux()
	var srv atomic.Pointer[kv.Server]
	raft.RegisterHandlersWithOptions(mux, func() *raft.Raft {
		if s := srv.Load(); s != nil {
			return s.Raft()
		}
		return nil
	}, raft.HandlerOptions{Token: peerToken, MaxBodyBytes: max(raft.DefaultMaxRPCBody, 2*int64(rcfg.SnapshotChunkSize))})

	opts := []kv.ServerOption{kv.WithMaxSessions(c.maxSessions)}
	if c.store == "lsm" {
		st, err := openLSMStore(fmt.Sprintf("%s/node-%d/lsm", c.dataDir, c.id))
		if err != nil {
			return nil, err
		}
		opts = append(opts, kv.WithStore(st))
	}
	s := kv.NewServer(c.id, len(c.peers), raft.NewSecureHTTPTransport(c.peers, peerTLS, peerToken),
		storage, c.maxState, rcfg, opts...)
	s.LogReads = c.logReads
	srv.Store(s)
	n.server = s

	api := http.NewServeMux()
	(&kv.API{Server: s, ClientURLs: c.peers, Timeout: 3 * time.Second,
		MaxKeyBytes: c.maxKeyBytes, MaxValueBytes: c.maxValueBytes}).Register(api)
	mux.Handle("/kv/", n.metrics.wrap("/kv", secure.RequireToken(clientToken, api)))

	mux.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(s.Raft().Status())
	})
	mux.HandleFunc("GET /metrics", n.writeMetrics)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) })
	mux.HandleFunc("GET /readyz", n.ready)
	n.handler = mux
	return n, nil
}

// peerTLSConfig is the client side of peer connections: nil (plain HTTP)
// without TLS; with TLS, it verifies peers against the CA and presents
// this node's certificate (needed when peers require mTLS).
func peerTLSConfig(f secure.TLSFiles) (*tls.Config, error) {
	if !f.Enabled() {
		return nil, nil
	}
	return secure.ClientConfig(f)
}

// ready reports whether this replica can usefully serve traffic: it knows
// a current leader (and, as a follower, heard from it recently) and its
// state machine is not far behind the commit index.
func (n *node) ready(w http.ResponseWriter, r *http.Request) {
	st := n.server.Raft().Status()
	var reason string
	switch {
	case st.Leader < 0:
		reason = "no known leader"
	case st.Role != "leader" && (st.SinceLeaderContact < 0 || st.SinceLeaderContact > 2*n.raftCfg.ElectionTimeoutMax):
		reason = fmt.Sprintf("no leader contact for %v", st.SinceLeaderContact)
	case st.CommitIndex-st.LastApplied > n.cfg.readyMaxLag:
		reason = fmt.Sprintf("apply lag %d entries", st.CommitIndex-st.LastApplied)
	}
	w.Header().Set("Content-Type", "application/json")
	if reason != "" {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	json.NewEncoder(w).Encode(map[string]any{"ready": reason == "", "reason": reason, "role": st.Role, "leader": st.Leader})
}

func (n *node) writeMetrics(w http.ResponseWriter, r *http.Request) {
	s := n.server
	st := s.Raft().Status()
	rm := s.Raft().Metrics()
	ks := s.Stats()
	isLeader := 0
	if st.Role == "leader" {
		isLeader = 1
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	gauge := func(name string, v any) { fmt.Fprintf(w, "# TYPE %s gauge\n%s %v\n", name, name, v) }
	counter := func(name string, v any) { fmt.Fprintf(w, "# TYPE %s counter\n%s %v\n", name, name, v) }
	gauge("raft_term", st.Term)
	gauge("raft_is_leader", isLeader)
	gauge("raft_commit_index", st.CommitIndex)
	gauge("raft_last_applied", st.LastApplied)
	gauge("raft_log_first_index", st.FirstIndex)
	gauge("raft_log_last_index", st.LastIndex)
	gauge("raft_state_bytes", n.storage.StateSize())
	gauge("raft_leader_contact_seconds", st.SinceLeaderContact.Seconds())
	counter("raft_elections_started_total", rm.ElectionsStarted)
	counter("raft_leader_terms_total", rm.LeaderTerms)
	counter("raft_snapshots_sent_total", rm.SnapshotsSent)
	counter("raft_snapshot_chunks_sent_total", rm.SnapshotChunksSent)
	counter("raft_snapshots_installed_total", rm.SnapshotsInstalled)
	counter("raft_snapshot_checksum_failures_total", rm.SnapshotChecksumFails)
	gauge("kv_keys", s.Len())
	gauge("kv_sessions", s.Sessions())
	counter("kv_applied_total", ks.Applied)
	counter("kv_snapshots_total", ks.Snapshots)
	counter("kv_snapshot_restores_total", ks.Restored)
	counter("kv_fast_reads_total", ks.FastReads)
	n.metrics.write(w)
}

func newLogger(format string) *slog.Logger {
	if format == "json" {
		return slog.New(slog.NewJSONHandler(os.Stderr, nil))
	}
	return slog.New(slog.NewTextHandler(os.Stderr, nil))
}

func newHTTPServer(c config, h http.Handler) (*http.Server, error) {
	srv := &http.Server{
		Addr:              c.listen,
		Handler:           h,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second, // bounds slow-body uploads
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       2 * time.Minute,
		MaxHeaderBytes:    64 << 10,
	}
	if c.tls.Enabled() {
		tc, err := secure.ServerConfig(c.tls)
		if err != nil {
			return nil, err
		}
		srv.TLSConfig = tc
	}
	return srv, nil
}

func main() {
	c, err := parseFlags(os.Args[1:])
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(0)
		}
		fmt.Fprintln(os.Stderr, "kvserver:", err)
		os.Exit(2)
	}
	logger := newLogger(c.logFormat)
	n, err := newNode(c, logger)
	if err != nil {
		logger.Error("startup failed", "err", err)
		os.Exit(1)
	}
	httpSrv, err := newHTTPServer(c, n.handler)
	if err != nil {
		logger.Error("tls setup failed", "err", err)
		os.Exit(1)
	}
	go func() {
		logger.Info("replica listening", "id", c.id, "addr", c.listen, "peers", c.peers,
			"tls", c.tls.Enabled(), "mtls", c.tls.CA != "",
			"peer_auth", c.peerTokenFile != "", "client_auth", c.clientTokenFile != "")
		var err error
		if httpSrv.TLSConfig != nil {
			err = httpSrv.ListenAndServeTLS("", "")
		} else {
			err = httpSrv.ListenAndServe()
		}
		if err != http.ErrServerClosed {
			logger.Error("listener failed", "err", err)
			os.Exit(1)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	logger.Info("shutting down")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	httpSrv.Shutdown(ctx)
	n.server.Kill()
}
