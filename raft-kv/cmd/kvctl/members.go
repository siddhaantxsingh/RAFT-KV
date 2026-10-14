package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/siddhaantxsingh/raft-kv/kv"
)

// members runs a membership command against the cluster, following
// 421 redirects to the leader.
func members(caller *kv.HTTPCaller, args []string) {
	if len(args) == 0 {
		usage()
	}
	var method, path, body string
	switch args[0] {
	case "list":
		method, path = http.MethodGet, "/admin/members"
	case "add":
		need(args, 3)
		method, path = http.MethodPost, "/admin/members"
		body = fmt.Sprintf(`{"id":%s,"addr":%q}`, args[1], args[2])
	case "remove":
		need(args, 2)
		method, path = http.MethodDelete, "/admin/members/"+args[1]
	default:
		usage()
	}
	client := caller.Client
	client.Timeout = 90 * time.Second
	urls := append([]string(nil), caller.URLs...)
	var lastErr string
	redirects := 0
	for attempt := 0; attempt < 3*len(urls)+3; attempt++ {
		u := urls[attempt%len(urls)]
		req, _ := http.NewRequest(method, u+path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			lastErr = err.Error()
			continue
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		switch {
		case resp.StatusCode == http.StatusOK:
			fmt.Print(string(b))
			return
		case resp.StatusCode == http.StatusMisdirectedRequest && resp.Header.Get("X-Raft-Leader") != "" && redirects < 5:
			redirects++
			urls = append([]string{resp.Header.Get("X-Raft-Leader")}, urls...)
			attempt = -1
			continue
		case resp.StatusCode == http.StatusServiceUnavailable || resp.StatusCode == http.StatusMisdirectedRequest:
			lastErr = strings.TrimSpace(string(b))
			time.Sleep(300 * time.Millisecond)
		default:
			fmt.Fprintf(os.Stderr, "error: status %d: %s", resp.StatusCode, b)
			os.Exit(1)
		}
	}
	fmt.Fprintln(os.Stderr, "error:", lastErr)
	os.Exit(1)
}
