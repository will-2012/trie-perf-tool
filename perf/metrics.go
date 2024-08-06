package main

import "github.com/ethereum/go-ethereum/metrics"

var (
	stateTrieGetLatency  = metrics.NewRegisteredTimer("statetrie/get/latency", nil)
	stateTriePutLatency  = metrics.NewRegisteredTimer("statetrie/put/latency", nil)
	VeraTrieGetLatency   = metrics.NewRegisteredTimer("veraTrie/get/latency", nil)
	VeraTriePutLatency   = metrics.NewRegisteredTimer("veraTrie/put/latency", nil)
	stateTrieHashLatency = metrics.NewRegisteredTimer("statetrie/hash/latency", nil)
	VeraTrieHashLatency  = metrics.NewRegisteredTimer("veraTrie/hash/latency", nil)

	stateDBCommitLatency = metrics.NewRegisteredTimer("statedb/commit/latency", nil)
	VeraDBCommitLatency  = metrics.NewRegisteredTimer("veradb/commit/latency", nil)

	StateDBGetLatency        = metrics.NewRegisteredTimer("statedb/get/latency", nil)
	StateDBAccPutLatency     = metrics.NewRegisteredTimer("statedb/account/put/latency", nil)
	StateDBStoragePutLatency = metrics.NewRegisteredTimer("statedb/storage/put/latency", nil)
	VersaDBGetLatency        = metrics.NewRegisteredTimer("versadb/get/latency", nil)
	versaDBPutLatency        = metrics.NewRegisteredTimer("versadb/put/latency", nil)

	stateTrieMemoryUsage = metrics.NewRegisteredGauge("statetrie/memory/usage", nil)
	VeraTrieMemoryUsage  = metrics.NewRegisteredGauge("veraTrie/memory/usage", nil)
	stateTrieGetTps      = metrics.NewRegisteredGauge("statetrie/get/tps", nil)
	stateTriePutTps      = metrics.NewRegisteredGauge("statetrie/put/tps", nil)
	VeraTrieGetTps       = metrics.NewRegisteredGauge("veraTrie/get/tps", nil)
	VeraTriePutTps       = metrics.NewRegisteredGauge("veraTrie/put/tps", nil)

	stateDBGetTps = metrics.NewRegisteredGauge("statedb/get/tps", nil)
	stateDBPutTps = metrics.NewRegisteredGauge("statedb/put/tps", nil)
	VeraDBGetTps  = metrics.NewRegisteredGauge("veradb/get/tps", nil)
	VeraDBPutTps  = metrics.NewRegisteredGauge("veradb/put/tps", nil)

	failGetCount   = metrics.NewRegisteredCounter("db/get/fail", nil)
	failWriteCount = metrics.NewRegisteredCounter("db/put/fail", nil)
)
