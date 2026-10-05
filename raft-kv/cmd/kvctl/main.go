// Command kvctl is a CLI client and load generator for kvserver.
//
//	kvctl -s http://localhost:7000,http://localhost:7001 put foo bar
//	kvctl get foo
//	kvctl bench -clients 32 -duration 10s -read-ratio 0.5
package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/siddhaantxsingh/raft-kv/kv"
	"github.com/siddhaantxsingh/raft-kv/secure"
)

func usage() {
	fmt.Fprintln(os.Stderr, `usage: kvctl [-s urls] <command> [args]
commands:
  get <key>
  put <key> <value>
  append <key> <value>
  delete <key>
  cas <key> <expected> <value>
  bench [-clients N] [-duration D] [-read-ratio R] [-keys K] [-value-size B]`)
	os.Exit(2)
}

func main() {
	servers := flag.String("s", envOr("KV_SERVERS", "http://127.0.0.1:7000,http://127.0.0.1:7001,http://127.0.0.1:7002"), "server URLs")
	var tf secure.TLSFiles
	flag.StringVar(&tf.CA, "tls-ca", os.Getenv("KV_TLS_CA"), "PEM CA bundle to verify servers (enables TLS)")
	flag.StringVar(&tf.Cert, "tls-cert", os.Getenv("KV_TLS_CERT"), "client certificate (for mTLS)")
	flag.StringVar(&tf.Key, "tls-key", os.Getenv("KV_TLS_KEY"), "client key (for mTLS)")
	tokenFile := flag.String("token-file", os.Getenv("KV_TOKEN_FILE"), "file holding the client bearer token")
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		usage()
	}
	token, err := secure.ReadToken(*tokenFile)
	check(err)
	var tlsCfg *tls.Config
	if tf.Enabled() {
		tlsCfg, err = secure.ClientConfig(tf)
		check(err)
	}
	caller := kv.NewSecureHTTPCaller(strings.Split(*servers, ","), tlsCfg, token)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	ck := kv.NewClerk(caller)

	switch args[0] {
	case "get":
		need(args, 2)
		v, found, err := ck.Get(ctx, args[1])
		check(err)
		if !found {
			fmt.Fprintln(os.Stderr, "(not found)")
			os.Exit(1)
		}
		fmt.Println(v)
	case "put":
		need(args, 3)
		check(ck.Put(ctx, args[1], args[2]))
	case "append":
		need(args, 3)
		check(ck.Append(ctx, args[1], args[2]))
	case "delete":
		need(args, 2)
		found, err := ck.Delete(ctx, args[1])
		check(err)
		fmt.Println(found)
	case "cas":
		need(args, 4)
		ok, cur, err := ck.CAS(ctx, args[1], args[2], args[3])
		check(err)
		fmt.Printf("swapped=%v current=%q\n", ok, cur)
	case "bench":
		bench(caller, args[1:])
	default:
		usage()
	}
}

func bench(caller kv.Caller, args []string) {
	fs := flag.NewFlagSet("bench", flag.ExitOnError)
	clients := fs.Int("clients", 16, "concurrent clients")
	duration := fs.Duration("duration", 10*time.Second, "test duration")
	readRatio := fs.Float64("read-ratio", 0.5, "fraction of operations that are reads")
	keys := fs.Int("keys", 1000, "key space size")
	valueSize := fs.Int("value-size", 64, "bytes per value")
	fs.Parse(args)

	value := strings.Repeat("v", *valueSize)
	var ops, errs atomic.Int64
	latMu := sync.Mutex{}
	var lats []time.Duration
	deadline := time.Now().Add(*duration)

	var wg sync.WaitGroup
	for c := 0; c < *clients; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ck := kv.NewClerk(caller)
			local := make([]time.Duration, 0, 4096)
			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			for time.Now().Before(deadline) {
				key := fmt.Sprintf("k%d", r.Intn(*keys))
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				t0 := time.Now()
				var err error
				if r.Float64() < *readRatio {
					_, _, err = ck.Get(ctx, key)
				} else {
					err = ck.Put(ctx, key, value)
				}
				cancel()
				if err != nil {
					errs.Add(1)
					continue
				}
				local = append(local, time.Since(t0))
				ops.Add(1)
			}
			latMu.Lock()
			lats = append(lats, local...)
			latMu.Unlock()
		}()
	}
	wg.Wait()

	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	pct := func(p float64) time.Duration {
		if len(lats) == 0 {
			return 0
		}
		return lats[int(float64(len(lats)-1)*p)]
	}
	fmt.Printf("clients=%d duration=%s read-ratio=%.2f\n", *clients, *duration, *readRatio)
	fmt.Printf("ops=%d errors=%d throughput=%.0f ops/s\n", ops.Load(), errs.Load(), float64(ops.Load())/duration.Seconds())
	fmt.Printf("latency p50=%s p95=%s p99=%s max=%s\n", pct(0.50), pct(0.95), pct(0.99), pct(1))
}

func need(args []string, n int) {
	if len(args) != n {
		usage()
	}
}

func check(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
