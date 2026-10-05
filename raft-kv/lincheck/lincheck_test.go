package lincheck

import "testing"

func TestSequentialOK(t *testing.T) {
	h := []Operation{
		{Kind: Put, Key: "x", Value: "1", Call: 0, Return: 1},
		{Kind: Get, Key: "x", OutValue: "1", OutFound: true, Call: 2, Return: 3},
		{Kind: Append, Key: "x", Value: "2", OutValue: "12", Call: 4, Return: 5},
	}
	if ok, _ := Check(h); !ok {
		t.Fatal("expected linearizable")
	}
}

func TestStaleRead(t *testing.T) {
	// Put(1) completes, then a later Get observes the key absent: not linearizable.
	h := []Operation{
		{Kind: Put, Key: "x", Value: "1", Call: 0, Return: 1},
		{Kind: Get, Key: "x", OutFound: false, Call: 2, Return: 3},
	}
	if ok, _ := Check(h); ok {
		t.Fatal("stale read accepted")
	}
}

func TestConcurrentEitherOrder(t *testing.T) {
	// Two overlapping puts; reads after both may see either, but two
	// sequential reads must not flip back and forth.
	h := []Operation{
		{ClientID: 0, Kind: Put, Key: "x", Value: "a", Call: 0, Return: 10},
		{ClientID: 1, Kind: Put, Key: "x", Value: "b", Call: 1, Return: 9},
		{ClientID: 2, Kind: Get, Key: "x", OutValue: "a", OutFound: true, Call: 11, Return: 12},
	}
	if ok, _ := Check(h); !ok {
		t.Fatal("expected linearizable")
	}
	h = append(h,
		Operation{ClientID: 2, Kind: Get, Key: "x", OutValue: "b", OutFound: true, Call: 13, Return: 14})
	if ok, _ := Check(h); ok {
		t.Fatal("flip-flopping reads accepted")
	}
}

func TestUnknownOutcome(t *testing.T) {
	// A timed-out Put may or may not have happened.
	h := []Operation{
		{Kind: Put, Key: "x", Value: "1", Call: 0, Unknown: true},
		{Kind: Get, Key: "x", OutValue: "1", OutFound: true, Call: 5, Return: 6},
	}
	if ok, _ := Check(h); !ok {
		t.Fatal("expected linearizable (put took effect)")
	}
	h[1] = Operation{Kind: Get, Key: "x", OutFound: false, Call: 5, Return: 6}
	if ok, _ := Check(h); !ok {
		t.Fatal("expected linearizable (put never took effect)")
	}
}

func TestCAS(t *testing.T) {
	h := []Operation{
		{Kind: CAS, Key: "l", Expected: "", Value: "A", OutOK: true, Call: 0, Return: 2},
		{Kind: CAS, Key: "l", Expected: "", Value: "B", OutOK: true, Call: 1, Return: 3},
	}
	if ok, _ := Check(h); ok {
		t.Fatal("two winners of the same lock accepted")
	}
}
