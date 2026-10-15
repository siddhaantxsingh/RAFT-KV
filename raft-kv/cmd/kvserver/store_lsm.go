//go:build lsm

package main

import (
	"github.com/siddhaantxsingh/raft-kv/kv"
	"github.com/siddhaantxsingh/raft-kv/kv/lsmstore"
)

func openLSMStore(dir string) (kv.StateStore, error) { return lsmstore.Open(dir) }
