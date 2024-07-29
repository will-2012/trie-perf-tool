package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	mathrand "math/rand"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

type Runner struct {
	db              TrieDatabase
	perfConfig      PerfConfig
	stat            *Stat
	lastStatInstant time.Time
	taskChan        chan map[string][]byte
	keyCache        *InsertedKeySet
	blockHeight     uint64
	rwDuration      time.Duration
	commitDuration  time.Duration
	hashDuration    time.Duration
}

func NewRunner(
	db TrieDatabase,
	config PerfConfig,
	taskBufferSize int, // Added a buffer size parameter for the task channel
) *Runner {
	runner := &Runner{
		db:              db,
		stat:            NewStat(),
		lastStatInstant: time.Now(),
		perfConfig:      config,
		taskChan:        make(chan map[string][]byte, taskBufferSize),
		keyCache:        NewFixedSizeSet(1000000),
	}

	return runner
}

func (r *Runner) Run(ctx context.Context) {
	defer close(r.taskChan)
	// Start task generation thread
	go generateTasks(ctx, r.taskChan, r.perfConfig.BatchSize)

	// init the trie key
	r.InitTrie()

	fmt.Println("init trie finish, begin to press kv")
	r.runInternal(ctx)
}

func generateTasks(ctx context.Context, taskChan chan<- map[string][]byte, batchSize uint64) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			taskMap := make(map[string][]byte, batchSize*2)
			address, acccounts := makeAccounts(int(batchSize))
			for i := 0; i < len(address); i++ {
				taskMap[string(crypto.Keccak256(address[i][:]))] = acccounts[i]
			}
			for i := 0; i < int(batchSize); i++ {
				randomStr := generateValue(32, 32)
				randomHash := common.BytesToHash(randomStr)
				path := generateValue(0, 64)
				taskMap[string(storageTrieNodeKey(randomHash, path))] = generateValue(7, 16)
			}

			taskChan <- taskMap
		}
	}
}

// randInt returns a random integer between min and max
func randInt(min, max int) int {
	return min + mathrand.Intn(max-min)
}

func generateRandomHexString() (string, error) {
	length := randInt(0, 65) // Length between 0 and 64 bytes
	if length == 0 {
		return "", nil
	}

	bytes := make([]byte, length)
	_, err := rand.Read(bytes)
	if err != nil {
		return "", err
	}

	return hex.EncodeToString(bytes), nil
}

func storageTrieNodeKey(accountHash common.Hash, path []byte) []byte {
	trieNodeStoragePrefix := []byte("O")
	buf := make([]byte, len(trieNodeStoragePrefix)+common.HashLength+len(path))
	n := copy(buf, trieNodeStoragePrefix)
	n += copy(buf[n:], accountHash.Bytes())
	copy(buf[n:], path)
	return buf
}

func (r *Runner) runInternal(ctx context.Context) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case taskInfo := <-r.taskChan:
			rwStart := time.Now()
			// read, put or delete keys
			r.UpdateTrie(taskInfo)
			r.rwDuration = time.Since(rwStart)

			// compute hash
			hashStart := time.Now()
			r.db.Hash()
			r.hashDuration = time.Since(hashStart)

			// commit
			commitStart := time.Now()
			if _, err := r.db.Commit(); err != nil {
				panic("failed to commit: " + err.Error())
			}
			r.blockHeight++
			r.commitDuration = time.Since(commitStart)

		case <-ticker.C:
			r.printStat()

		case <-ctx.Done():
			fmt.Println("Shutting down")
			return
		}
	}
}

func (r *Runner) printStat() {
	delta := time.Since(r.lastStatInstant)
	fmt.Printf(
		"[%s] Perf In Progress %s, block height=%d elapsed: [rw=%v, commit=%v, cal hash=%v]\n",
		time.Now().Format(time.RFC3339Nano),
		r.stat.CalcTpsAndOutput(delta),
		r.blockHeight,
		r.rwDuration,
		r.commitDuration,
		r.hashDuration,
	)
	r.lastStatInstant = time.Now()
}

func (r *Runner) InitTrie() {
	addresses, accounts := makeAccounts(int(r.perfConfig.BatchSize) * 100)

	for i := 0; i < len(addresses); i++ {
		err := r.db.Put(crypto.Keccak256(addresses[i][:]), accounts[i])
		if err != nil {
			panic("init trie err" + err.Error())
		}
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

	// simulate insert and delete trie
	for key, value := range taskInfo {
		keyName := []byte(key)
		err := r.db.Put(keyName, value)
		if err != nil {
			fmt.Println("fail to insert key to trie", "key", string(keyName),
				"err", err.Error())
		}
		r.keyCache.Add(key)
		r.stat.IncPut(1)

		if randomFloat() < r.perfConfig.DeleteRatio {
			// delete the key from inserted key cache
			randomKey, found := r.keyCache.RandomItem()
			if found {
				keyName = []byte(randomKey)
				err = r.db.Delete(keyName)
				if err != nil {
					fmt.Println("fail to delete key to trie", "key", string(keyName),
						"err", err.Error())
				}
				r.stat.IncDelete(1)
			}
		}
	}
}
