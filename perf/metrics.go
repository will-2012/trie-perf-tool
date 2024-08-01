package main

import "github.com/ethereum/go-ethereum/metrics"

var (
	getLatency  = metrics.NewRegisteredTimer("db/get/latency", nil)
	putLatency  = metrics.NewRegisteredTimer("db/put/latency", nil)
	memoryUsage = metrics.NewRegisteredGauge("go/memory/usage", nil)
	getTps      = metrics.NewRegisteredGauge("db/get/tps", nil)
	putTps      = metrics.NewRegisteredGauge("db/put /tps", nil)
)
