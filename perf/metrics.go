package main

import "github.com/ethereum/go-ethereum/metrics"

var (
	stateTrieGetLatency  = metrics.NewRegisteredTimer("statetrie/get/latency", nil)
	stateTriePutLatency  = metrics.NewRegisteredTimer("statetrie/put/latency", nil)
	VeraTrieGetLatency   = metrics.NewRegisteredTimer("veraTrie/get/latency", nil)
	VeraTriePutLatency   = metrics.NewRegisteredTimer("veraTrie/put/latency", nil)
	stateTrieHashLatency = metrics.NewRegisteredTimer("statetrie/hash/latency", nil)
	VeraTrieHashLatency  = metrics.NewRegisteredTimer("veraTrie/hash/latency", nil)

	stateTrieMemoryUsage = metrics.NewRegisteredGauge("statetrie/memory/usage", nil)
	VeraTrieMemoryUsage  = metrics.NewRegisteredGauge("veraTrie/memory/usage", nil)
	stateTrieGetTps      = metrics.NewRegisteredGauge("statetrie/get/tps", nil)
	stateTriePutTps      = metrics.NewRegisteredGauge("statetrie/put/tps", nil)
	VeraTrieGetTps       = metrics.NewRegisteredGauge("veraTrie/get/tps", nil)
	VeraTriePutTps       = metrics.NewRegisteredGauge("veraTrie/put/tps", nil)

	failGetCount   = metrics.NewRegisteredCounter("db/get/fail", nil)
	failWriteCount = metrics.NewRegisteredCounter("db/put/fail", nil)
)
