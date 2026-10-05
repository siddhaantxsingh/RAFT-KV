package main

import (
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"time"
)

// latencyBuckets are histogram upper bounds in seconds.
var latencyBuckets = []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5}

type histogram struct {
	counts []uint64 // per bucket, plus +Inf at the end
	sum    float64
	n      uint64
}

// requestMetrics records per-route request counts by status class and a
// latency histogram, exposed in Prometheus text format (no dependencies).
type requestMetrics struct {
	mu     sync.Mutex
	counts map[[3]string]uint64 // route, method, code
	hist   map[[2]string]*histogram
}

func newRequestMetrics() *requestMetrics {
	return &requestMetrics{counts: map[[3]string]uint64{}, hist: map[[2]string]*histogram{}}
}

type statusWriter struct {
	http.ResponseWriter
	code int
}

func (w *statusWriter) WriteHeader(c int) { w.code = c; w.ResponseWriter.WriteHeader(c) }

func (m *requestMetrics) wrap(route string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, code: 200}
		next.ServeHTTP(sw, r)
		m.observe(route, r.Method, sw.code, time.Since(start))
	})
}

func (m *requestMetrics) observe(route, method string, code int, d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.counts[[3]string{route, method, fmt.Sprint(code)}]++
	k := [2]string{route, method}
	h := m.hist[k]
	if h == nil {
		h = &histogram{counts: make([]uint64, len(latencyBuckets)+1)}
		m.hist[k] = h
	}
	sec := d.Seconds()
	i := sort.SearchFloat64s(latencyBuckets, sec)
	h.counts[i]++
	h.sum += sec
	h.n++
}

func (m *requestMetrics) write(w io.Writer) {
	m.mu.Lock()
	defer m.mu.Unlock()
	fmt.Fprintln(w, "# TYPE kv_http_requests_total counter")
	keys := make([][3]string, 0, len(m.counts))
	for k := range m.counts {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return fmt.Sprint(keys[i]) < fmt.Sprint(keys[j]) })
	for _, k := range keys {
		fmt.Fprintf(w, "kv_http_requests_total{route=%q,method=%q,code=%q} %d\n", k[0], k[1], k[2], m.counts[k])
	}
	fmt.Fprintln(w, "# TYPE kv_http_request_duration_seconds histogram")
	hk := make([][2]string, 0, len(m.hist))
	for k := range m.hist {
		hk = append(hk, k)
	}
	sort.Slice(hk, func(i, j int) bool { return fmt.Sprint(hk[i]) < fmt.Sprint(hk[j]) })
	for _, k := range hk {
		h := m.hist[k]
		var cum uint64
		for i, b := range latencyBuckets {
			cum += h.counts[i]
			fmt.Fprintf(w, "kv_http_request_duration_seconds_bucket{route=%q,method=%q,le=\"%g\"} %d\n", k[0], k[1], b, cum)
		}
		cum += h.counts[len(latencyBuckets)]
		fmt.Fprintf(w, "kv_http_request_duration_seconds_bucket{route=%q,method=%q,le=\"+Inf\"} %d\n", k[0], k[1], cum)
		fmt.Fprintf(w, "kv_http_request_duration_seconds_sum{route=%q,method=%q} %g\n", k[0], k[1], h.sum)
		fmt.Fprintf(w, "kv_http_request_duration_seconds_count{route=%q,method=%q} %d\n", k[0], k[1], h.n)
	}
}
