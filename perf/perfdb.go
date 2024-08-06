package main

import (
	"context"
	"fmt"
	mathrand "math/rand"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
)

type DBRunner struct {
	db                StateDatabase
	perfConfig        PerfConfig
	stat              *Stat
	lastStatInstant   time.Time
	taskChan          chan DBTask
	keyCache          *InsertedKeySet
	blockHeight       uint64
	rwDuration        time.Duration
	rDuration         time.Duration
	wDuration         time.Duration
	commitDuration    time.Duration
	hashDuration      time.Duration
	totalRwDurations  time.Duration // Accumulated rwDuration
	totalReadCost     time.Duration
	totalWriteCost    time.Duration
	BlockCount        int64 // Number of rwDuration samples
	totalHashurations time.Duration
}

func NewDBRunner(
	db StateDatabase,
	config PerfConfig,
	taskBufferSize int, // Added a buffer size parameter for the task channel
) *DBRunner {
	runner := &DBRunner{
		db:              db,
		stat:            NewStat(),
		lastStatInstant: time.Now(),
		perfConfig:      config,
		taskChan:        make(chan DBTask, taskBufferSize),
		keyCache:        NewFixedSizeSet(1000000),
	}

	return runner
}

func (d *DBRunner) Run(ctx context.Context) {
	defer close(d.taskChan)
	// Start task generation thread
	go generateStorageTasks(ctx, d.taskChan, d.perfConfig.BatchSize/2)
	// init the state db
	d.InitDB()

	fmt.Println("init db finish, begin to press kv")
	d.runInternal(ctx)
}

func generateStorageTasks(ctx context.Context, taskChan chan<- DBTask, batchSize uint64) {
	for {
		select {
		case <-ctx.Done():
			return
		default:

			taskMap := make(DBTask, batchSize)
			random := mathrand.New(mathrand.NewSource(0))

			address, accounts := makeAccounts(int(batchSize) / 2)
			for i := 0; i < len(address); i++ {
				taskMap[string(crypto.Keccak256(address[i][:]))] = CAKeyValue{
					Account: accounts[i]}
			}

			CAAccount := make([][20]byte, batchSize/2)
			for i := 0; i < len(CAAccount); i++ {
				data := make([]byte, 20)
				random.Read(data)
				mathrand.Seed(time.Now().UnixNano())
				mathrand.Shuffle(len(data), func(i, j int) { data[i], data[j] = data[j], data[i] })
				copy(CAAccount[i][:], data)
			}

			keys := make([]string, 0, CAStorageSize)
			vals := make([]string, 0, CAStorageSize)

			for i := 0; i < len(CAAccount); i++ {
				for j := 0; j < CAStorageSize; j++ {
					randomStr := generateValue(32, 32)
					//	randomHash := common.BytesToHash(randomStr)
					//	path := generateValue(0, 64)
					//	key := string(storageTrieNodeKey(randomHash, path))
					value := generateValue(7, 16)
					keys = append(keys, string(randomStr))
					vals = append(vals, string(value))
				}
				taskMap[string(crypto.Keccak256(CAAccount[i][:]))] = CAKeyValue{
					Keys: keys, Vals: vals}
			}

			taskChan <- taskMap
		}
	}
}

func (r *DBRunner) runInternal(ctx context.Context) {
	startTime := time.Now()
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	printAvg := 0

	for {
		select {
		case taskInfo := <-r.taskChan:
			rwStart := time.Now()
			// read, put or delete keys
			r.UpdateAccount(taskInfo)
			r.rwDuration = time.Since(rwStart)
			r.totalRwDurations += r.rwDuration
			// compute hash
			hashStart := time.Now()
			r.db.Hash()
			r.hashDuration = time.Since(hashStart)
			/*
				if r.db.GetMPTEngine() == VERSADBEngine {
					VeraTrieHashLatency.Update(r.hashDuration)
				} else {
					stateTrieHashLatency.Update(r.hashDuration)
				}
			*/

			r.totalHashurations += r.hashDuration
			// commit
			if _, err := r.db.Commit(); err != nil {
				panic("failed to commit: " + err.Error())
			}

			r.blockHeight++
			//r.commitDuration = time.Since(commitStart)

		case <-ticker.C:
			r.printStat()
			printAvg++
			if printAvg%100 == 0 {
				r.printAVGStat(startTime)
			}

		case <-ctx.Done():
			fmt.Println("Shutting down")
			r.printAVGStat(startTime)
			return
		}
	}
}

func (r *DBRunner) printAVGStat(startTime time.Time) {
	fmt.Printf(
		" Avg Perf metrics: %s, block height=%d elapsed: [read=%v us, write=%v ms, cal hash=%v us]\n",
		r.stat.CalcAverageIOStat(time.Since(startTime)),
		r.blockHeight,
		float64(r.totalReadCost.Microseconds())/float64(r.blockHeight),
		float64(r.totalWriteCost.Microseconds())/float64(r.blockHeight),
		float64(r.totalHashurations.Milliseconds())/float64(r.blockHeight),
	)
}

func (r *DBRunner) printStat() {
	delta := time.Since(r.lastStatInstant)
	fmt.Printf(
		"[%s] Perf In Progress %s, block height=%d elapsed: [batch read=%v, batch write=%v, commit=%v, cal hash=%v]\n",
		time.Now().Format(time.RFC3339Nano),
		r.stat.CalcTpsAndOutput(delta),
		r.blockHeight,
		r.rDuration,
		r.wDuration,
		r.commitDuration,
		r.hashDuration,
	)
	r.lastStatInstant = time.Now()
}

func (r *DBRunner) InitDB() {
	addresses, accounts := makeAccounts(int(r.perfConfig.BatchSize) * 100)

	for i := 0; i < len(addresses); i++ {
		r.db.AddAccount(string(crypto.Keccak256(addresses[i][:])), accounts[i])
		r.keyCache.Add(string(crypto.Keccak256(addresses[i][:])))
		// double write to leveldb
	}

	fmt.Println("init db account suceess")

	val, _ := r.db.GetAccount(string(crypto.Keccak256(addresses[0][:])))
	fmt.Println(" get cache key1", val)
	if _, err := r.db.Commit(); err != nil {
		panic("failed to commit: " + err.Error())
	}
	val, _ = r.db.GetAccount(string(crypto.Keccak256(addresses[0][:])))
	fmt.Println(" get cache key2", val)
}

func (d *DBRunner) UpdateAccount(
	taskInfo DBTask,
) {
	// todo make it as config
	batchSize := int(d.perfConfig.BatchSize)

	var wg sync.WaitGroup
	start := time.Now()
	// simulate parallel read
	for i := 0; i < d.perfConfig.NumJobs; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// random read of local recently cache of inserted keys
			for j := 0; j < batchSize; j++ {
				randomKey, found := d.keyCache.RandomItem()
				if found {
					var value []byte
					value, err := d.db.GetAccount(randomKey)
					fmt.Println("get account value", "key", randomKey, "value:", value)
					d.stat.IncGet(1)
					if err != nil || value == nil {
						if err != nil {
							fmt.Println("fail to get key", err.Error())
						}
						d.stat.IncGetNotExist(1)
					}
				}
			}
		}()
	}
	d.rDuration = time.Since(start)
	d.totalReadCost += d.rDuration
	wg.Wait()
	/*
		if d.db.GetMPTEngine() == VERSADBEngine {
			VeraTrieGetTps.Update(int64(r.perfConfig.NumJobs*batchSize) / r.rDuration.Microseconds())
		} else {
			stateTrieGetTps.Update(int64(r.perfConfig.NumJobs*batchSize) / r.rDuration.Microseconds())
		}

	*/

	start = time.Now()
	// simulate insert Account and Storage Trie
	for key, value := range taskInfo {
		startPut := time.Now()
		if len(value.Account) > 0 {
			d.db.AddAccount(key, value.Account)
			StateDBAccPutLatency.Update(time.Since(startPut))
			d.keyCache.Add(key)
			fmt.Println("sucess to update account ")
		} else {
			d.db.AddStorage([]byte(key), value.Keys, value.Vals)
			StateDBStoragePutLatency.Update(time.Since(startPut))
			fmt.Println("sucess to update storage ")
		}
		d.stat.IncPut(1)
	}
	d.wDuration = time.Since(start)
	d.totalWriteCost += d.wDuration
	/*
		if r.db.GetMPTEngine() == VERSADBEngine {
			VeraTriePutTps.Update(int64(batchSize) / r.wDuration.Milliseconds())
		} else {
			stateTriePutTps.Update(int64(batchSize) / r.wDuration.Milliseconds())
		}

	*/

}
