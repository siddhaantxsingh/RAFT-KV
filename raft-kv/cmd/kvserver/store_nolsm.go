//go:build !lsm

package main

import (
	"errors"

	"github.com/siddhaantxsingh/raft-kv/kv"
)

func openLSMStore(string) (kv.StateStore, error) {
	return nil, errors.New("-store lsm requires a binary built with -tags lsm (see docs/RAFT_LSM_INTEGRATION.md)")
}
