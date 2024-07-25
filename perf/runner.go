package main

import (
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
)

type Runner struct {
	db              TrieDatabase
	perfConfig      PerfConfig
	stat            *Stat
	lastStatTime    sync.Mutex
	lastStatInstant time.Time
	taskChan        chan map[string][]byte
	keyCache        *InsertedKeySet
	blockHeight     uint64
}

func NewRunner(
	db TrieDatabase,
	batchSize uint64,
	numJobs int,
	keyRange uint64,
	minValueSize uint64,
	maxValueSize uint64,
	deleteRatio float64,
	taskBufferSize int, // Added a buffer size parameter for the task channel
) *Runner {
	runner := &Runner{
		db:              db,
		stat:            NewStat(),
		lastStatInstant: time.Now(),
		perfConfig: PerfConfig{
			BatchSize:    batchSize,
			NumJobs:      numJobs,
			KeyRange:     keyRange,
			MinValueSize: minValueSize,
			MaxValueSize: maxValueSize,
			DeleteRatio:  deleteRatio,
		},
		taskChan: make(chan map[string][]byte, taskBufferSize),
		keyCache: NewFixedSizeSet(1000000),
	}

	return runner
}

func (r *Runner) Run() {
	running := &atomic.Bool{}
	running.Store(true)

	// Listen for Ctrl+C signal
	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, os.Interrupt)
		<-ch
		running.Store(false)
	}()

	// Start task generation thread
	go r.generateTasks(running)

	// init the trie key
	r.InitTrie()
	fmt.Println("init trie finish")
	// Execute the function in an infinite loop until Ctrl+C signal is received
	for running.Load() {
		//for i := 0; i < 1; i++ {
		r.runInternal()
	}
}

func (r *Runner) generateTasks(running *atomic.Bool) {
	for running.Load() {
		taskMap := make(map[string][]byte)
		address, acccounts := makeAccounts(int(r.perfConfig.BatchSize))
		for i := 0; i < len(address); i++ {
			taskMap[string(crypto.Keccak256(address[i][:]))] = acccounts[i]
		}
		r.taskChan <- taskMap
	}
}

func (r *Runner) runInternal() {
	rwStart := time.Now()

	taskInfo := <-r.taskChan

	r.UpdateTrie(taskInfo)
	rwDuration := time.Since(rwStart)

	// compute hash
	hashStart := time.Now()
	r.db.Hash()
	hashDuration := time.Since(hashStart)

	// commit
	commitStart := time.Now()
	if _, err := r.db.Commit(); err != nil {
		panic("failed to commit: " + err.Error())
	}
	r.blockHeight++
	commitDuration := time.Since(commitStart)

	// Notify task consumption complete
	select {
	case r.taskChan <- nil:
	default:
	}

	totalDuration := time.Since(rwStart)
	// print stat
	r.lastStatTime.Lock()
	delta := time.Since(r.lastStatInstant)
	if delta >= 3*time.Second {
		fmt.Printf(
			"[%s] Perf In Progress %s, block height=%d elapsed: [rw=%v, commit=%v, cal hash=%v, total cost=%v]\n",
			time.Now().Format(time.RFC3339Nano),
			r.stat.CalcTpsAndOutput(delta),
			r.blockHeight,
			rwDuration,
			commitDuration,
			hashDuration,
			totalDuration,
		)
		r.lastStatInstant = time.Now()
	}
	r.lastStatTime.Unlock()
}

func (r *Runner) InitTrie() {
	addresses, accounts := makeAccounts(int(r.perfConfig.BatchSize) * 100)

	for i := 0; i < len(addresses); i++ {
		r.db.Put(crypto.Keccak256(addresses[i][:]), accounts[i])
		r.keyCache.Add(string(crypto.Keccak256(addresses[i][:])))
	}

	// commit
	if _, err := r.db.Commit(); err != nil {
		panic("failed to commit: " + err.Error())
	}
}

func (r *Runner) UpdateTrie(
	taskInfo map[string][]byte,
) {
	var wg sync.WaitGroup
	// simulate parallel read
	fmt.Println("read begin")
	for i := 0; i < r.perfConfig.NumJobs; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// random read of local recently cache of inserted keys
			for j := 0; j < int(r.perfConfig.BatchSize)/r.perfConfig.NumJobs; j++ {
				randomKey, found := r.keyCache.RandomItem()
				if found {
					keyBytes := []byte(randomKey)
					if value, err := r.db.Get(keyBytes); err == nil {
						r.stat.IncGet(1)
						if value == nil {
							r.stat.IncGetNotExist(1)
						}
					}
				}
			}
		}()
	}
	wg.Wait()
	fmt.Println("read finish")
	// simulate insert and delete trie
	for key, value := range taskInfo {
		keyName := []byte(key)
		if randomFloat() > r.perfConfig.DeleteRatio {
			err := r.db.Put(keyName, value)
			if err != nil {
				fmt.Println("fail to insert key to trie", "key", string(keyName),
					"err", err.Error())
			}
			r.keyCache.Add(key)
			r.stat.IncPut(1)
		} else {
			err := r.db.Delete(keyName)
			if err != nil {
				fmt.Println("fail to delete key to trie", "key", string(keyName),
					"err", err.Error())
			}
			r.stat.IncDelete(1)
		}
	}
	fmt.Println("insert finish")
}

func generateKey(keyRange uint64) []byte {
	return []byte(fmt.Sprintf("%016x", rand.Uint64()%keyRange))
}

func generateValue(minSize, maxSize uint64) []byte {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	size := minSize + uint64(rand.Intn(int(maxSize-minSize+1)))
	b := make([]byte, size)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return b
}
