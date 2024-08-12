package main

import (
	"context"
	"fmt"
	mathrand "math/rand"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/holiman/uint256"
)

type DBRunner struct {
	db              StateDatabase
	perfConfig      PerfConfig
	stat            *Stat
	lastStatInstant time.Time
	taskChan        chan DBTask
	initTaskChan    chan DBTask
	keyCache        *InsertedKeySet
	ownerCache      *InsertedKeySet
	//storageCache      *lru.Cache[string, []byte]
	storageCache      map[string][]string
	largeStorageCache map[string][]string
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
	updateAccount     int64
	largeStorageTrie  []string
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
		initTaskChan:    make(chan DBTask, taskBufferSize),
		keyCache:        NewFixedSizeSet(1000000),
		ownerCache:      NewFixedSizeSet(CAStorageTrieNum),
		//	storageCache:    lru.NewCache[string, []byte](100000),
		storageCache:     make(map[string][]string),
		largeStorageTrie: make([]string, 2),
	}

	return runner
}

func (d *DBRunner) Run(ctx context.Context) {
	defer close(d.taskChan)
	// init the state db
	d.InitAccount()
	d.InitStorageTrie(d.generateInitStorageTasks())
	fmt.Println("init db finish, begin to press kv")
	// Start task generation thread
	go d.generateRunTasks(ctx, d.perfConfig.BatchSize)
	d.runInternal(ctx)
}

func (d *DBRunner) generateRunTasks(ctx context.Context, batchSize uint64) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			taskMap := NewDBTask()
			random := mathrand.New(mathrand.NewSource(0))
			updateAccounts := int(batchSize) / 5
			accounts := make([][]byte, updateAccounts)
			for i := 0; i < updateAccounts; i++ {
				var (
					nonce = uint64(random.Int63())
					root  = types.EmptyRootHash
					code  = crypto.Keccak256(generateRandomBytes(20))
				)
				numBytes := random.Uint32() % 33 // [0, 32] bytes
				balanceBytes := make([]byte, numBytes)
				random.Read(balanceBytes)
				balance := new(uint256.Int).SetBytes(balanceBytes)
				data, _ := rlp.EncodeToBytes(&types.StateAccount{Nonce: nonce, Balance: balance, Root: root, CodeHash: code})
				accounts[i] = data
			}

			for i := 0; i < updateAccounts; i++ {
				randomKey, found := d.keyCache.RandomItem()
				if found {
					// update the account
					taskMap.AccountTask[randomKey] = accounts[i]
				}
			}

			// small storage trie write 3/5 kv of storage
			storageUpdateNum := int(batchSize) / 5 * 3 / (CAStorageTrieNum - 2)
			//	StorageInitSize := d.perfConfig.StorageTrieSize
			for k, v := range d.storageCache {
				keys := make([]string, 0, storageUpdateNum)
				vals := make([]string, 0, storageUpdateNum)
				for j := 0; j < storageUpdateNum; j++ {
					// only cache 10000 for updating test
					randomIndex := mathrand.Intn(10000)
					value := generateValue(7, 16)
					keys = append(keys, v[randomIndex])
					vals = append(vals, string(value))
				}
				taskMap.SmallStorageTask[k] = CAKeyValue{Keys: keys, Vals: vals}
			}
			// large storage trie write 1/5 kv of storage
			largeStorageUpdateNum := int(batchSize) / 5 / 2
			for k, v := range d.largeStorageCache {
				keys := make([]string, 0, storageUpdateNum)
				vals := make([]string, 0, storageUpdateNum)
				for j := 0; j < largeStorageUpdateNum; j++ {
					// only cache 10000 for updating test
					randomIndex := mathrand.Intn(10000)
					value := generateValue(7, 16)
					keys = append(keys, v[randomIndex])
					vals = append(vals, string(value))
				}
				taskMap.LargeStorageTask[k] = CAKeyValue{Keys: keys, Vals: vals}
			}

			d.taskChan <- taskMap
		}
	}
}

func (d *DBRunner) generateInitStorageTasks() InitDBTask {
	taskMap := make(InitDBTask, CAStorageTrieNum)
	//taskMap2 := make(InitDBTask, CAStorageTrieNum-2)
	random := mathrand.New(mathrand.NewSource(0))

	CAAccount := make([][20]byte, CAStorageTrieNum)
	for i := 0; i < len(CAAccount); i++ {
		data := make([]byte, 20)
		random.Read(data)
		mathrand.Seed(time.Now().UnixNano())
		mathrand.Shuffle(len(data), func(i, j int) { data[i], data[j] = data[j], data[i] })
		copy(CAAccount[i][:], data)
	}

	StorageInitSize := d.perfConfig.StorageTrieSize
	// init large tree by the config trie size
	for i := 0; i < 2; i++ {
		ownerHash := string(crypto.Keccak256(CAAccount[i][:]))
		keys := make([]string, 0, StorageInitSize)
		vals := make([]string, 0, StorageInitSize)
		for j := uint64(0); j < StorageInitSize; j++ {
			randomStr := generateValue(32, 32)
			value := generateValue(7, 16)
			keys = append(keys, string(randomStr))
			vals = append(vals, string(value))
		}
		taskMap[ownerHash] = CAKeyValue{
			Keys: keys, Vals: vals}
		d.largeStorageTrie[i] = ownerHash
	}

	// init small tree by the config trie size
	StorageInitSize = StorageInitSize / 100
	for i := 0; i < len(CAAccount)-2; i++ {
		ownerHash := string(crypto.Keccak256(CAAccount[i][:]))

		keys := make([]string, 0, StorageInitSize)
		vals := make([]string, 0, StorageInitSize)
		for j := uint64(0); j < StorageInitSize; j++ {
			randomStr := generateValue(32, 32)
			value := generateValue(7, 16)
			keys = append(keys, string(randomStr))
			vals = append(vals, string(value))
		}
		taskMap[ownerHash] = CAKeyValue{
			Keys: keys, Vals: vals}
	}

	return taskMap
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
			r.UpdateDB(taskInfo)
			r.rwDuration = time.Since(rwStart)
			r.totalRwDurations += r.rwDuration
			// compute hash
			hashStart := time.Now()
			r.db.Hash()
			r.hashDuration = time.Since(hashStart)
			if r.db.GetMPTEngine() == VERSADBEngine {
				VeraDBHashLatency.Update(r.hashDuration)
			} else {
				stateDBHashLatency.Update(r.hashDuration)
			}

			r.totalHashurations += r.hashDuration
			// commit
			commtStart := time.Now()
			if _, err := r.db.Commit(); err != nil {
				panic("failed to commit: " + err.Error())
			}

			r.commitDuration = time.Since(commtStart)
			if r.db.GetMPTEngine() == VERSADBEngine {
				VeraDBCommitLatency.Update(r.commitDuration)
			} else {
				stateDBCommitLatency.Update(r.commitDuration)
			}

			r.blockHeight++
			r.updateAccount = 0
			BlockHeight.Update(int64(r.blockHeight))
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
		" Avg Perf metrics: %s, block height=%d elapsed: [read =%v us, write=%v ms, cal hash=%v us]\n",
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

func (r *DBRunner) InitAccount() {
	addresses, accounts := makeAccounts(InitAccounts)

	for i := 0; i < len(addresses); i++ {
		initKey := string(crypto.Keccak256(addresses[i][:]))
		r.db.AddAccount(initKey, accounts[i])
		r.keyCache.Add(initKey)
		if r.db.GetMPTEngine() == StateTrieEngine && r.db.GetFlattenDB() != nil {
			// simulate insert key to snap
			snapDB := r.db.GetFlattenDB()
			rawdb.WriteAccountSnapshot(snapDB, common.BytesToHash([]byte(initKey)), accounts[i])
		}
		// double write to leveldb
	}

	if _, err := r.db.Commit(); err != nil {
		panic("failed to commit: " + err.Error())
	}
	fmt.Println("init db account commit suceess")
}

func (d *DBRunner) UpdateDB(
	taskInfo DBTask,
) {
	// todo make it as config
	batchSize := int(d.perfConfig.BatchSize)
	threadNum := d.perfConfig.NumJobs
	var wg sync.WaitGroup
	start := time.Now()
	smallTrieReadNum := batchSize / 5 * 3 / (CAStorageTrieNum - 2)
	largeTrieReadNum := batchSize / 5 / 2
	ownerList, err := d.ownerCache.GetNRandomSets(threadNum-1, (CAStorageTrieNum-2)/(threadNum-1))
	if err != nil {
		panic("error split owner" + err.Error())
	}

	// use threads to read small storage tries
	for i := 0; i < threadNum-1; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			for t := 0; t < len(ownerList[index]); t++ {
				for j := 0; j < smallTrieReadNum; j++ {
					owner := ownerList[index][t]
					keys := d.storageCache[owner]
					randomIndex := mathrand.Intn(len(keys))
					startRead := time.Now()
					value, err := d.db.GetStorage([]byte(owner), []byte(keys[randomIndex]))
					if d.db.GetMPTEngine() == VERSADBEngine {
						versaDBStorageGetLatency.Update(time.Since(startRead))
					} else {
						StateDBStorageGetLatency.Update(time.Since(startRead))
					}
					d.stat.IncGet(1)
					if err != nil || value == nil {
						if err != nil {
							fmt.Println("fail to get key", err.Error())
						}
						d.stat.IncGetNotExist(1)
					}
				}
			}
		}(i)
	}

	// use one thread to read large storage tries
	wg.Add(1)
	go func() {
		defer wg.Done()
		index := mathrand.Intn(1)
		for j := 0; j < largeTrieReadNum; j++ {
			owner := d.largeStorageTrie[index]
			keys := d.largeStorageCache[owner]
			randomIndex := mathrand.Intn(len(keys))
			startRead := time.Now()
			value, err := d.db.GetStorage([]byte(owner), []byte(keys[randomIndex]))
			if d.db.GetMPTEngine() == VERSADBEngine {
				versaDBStorageGetLatency.Update(time.Since(startRead))
			} else {
				StateDBStorageGetLatency.Update(time.Since(startRead))
			}
			d.stat.IncGet(1)
			if err != nil || value == nil {
				if err != nil {
					fmt.Println("fail to get key", err.Error())
				}
				d.stat.IncGetNotExist(1)
			}
		}
	}()

	wg.Wait()

	//  read 1/5 kv of account
	for i := 0; i < threadNum; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// random read 2/5 kv from account
			for j := 0; j < batchSize/5; j++ {
				randomKey, found := d.keyCache.RandomItem()
				if found {
					var value []byte
					startRead := time.Now()
					value, err := d.db.GetAccount(randomKey)
					if d.db.GetMPTEngine() == VERSADBEngine {
						VersaDBAccGetLatency.Update(time.Since(startRead))
					} else {
						StateDBAccGetLatency.Update(time.Since(startRead))
					}
					d.stat.IncGet(1)
					if err != nil || value == nil {
						if err != nil {
							fmt.Println("fail to get key", err.Error())
						}
						d.stat.IncGetNotExist(1)
					}
				} else {
					fmt.Println("fail to get account key")
				}
			}
		}()
	}
	wg.Wait()

	d.rDuration = time.Since(start)
	d.totalReadCost += d.rDuration

	if d.db.GetMPTEngine() == VERSADBEngine {
		VeraDBGetTps.Update(int64(d.perfConfig.NumJobs*batchSize) / d.rDuration.Milliseconds())
	} else {
		stateDBGetTps.Update(int64(d.perfConfig.NumJobs*batchSize) / d.rDuration.Milliseconds())
	}

	start = time.Now()

	// simulate upadte small Storage Trie
	for key, value := range taskInfo.SmallStorageTask {
		startPut := time.Now()
		// add new storage
		err = d.db.UpdateStorage([]byte(key), value.Keys, value.Vals)
		if err != nil {
			fmt.Println("update storage err", err.Error())
		}
		microseconds := time.Since(startPut).Microseconds() / int64(len(value.Keys))
		if d.db.GetMPTEngine() == VERSADBEngine {
			versaDBStoragePutLatency.Update(time.Duration(microseconds) * time.Microsecond)
		} else {
			StateDBStoragePutLatency.Update(time.Duration(microseconds) * time.Microsecond)
		}
		d.stat.IncPut(uint64(len(value.Keys)))
	}

	// simulate update large Storage Trie
	for key, value := range taskInfo.LargeStorageTask {
		startPut := time.Now()
		// add new storage
		err = d.db.UpdateStorage([]byte(key), value.Keys, value.Vals)
		if err != nil {
			fmt.Println("update storage err", err.Error())
		}
		microseconds := time.Since(startPut).Microseconds() / int64(len(value.Keys))
		if d.db.GetMPTEngine() == VERSADBEngine {
			versaDBStoragePutLatency.Update(time.Duration(microseconds) * time.Microsecond)
		} else {
			StateDBStoragePutLatency.Update(time.Duration(microseconds) * time.Microsecond)
		}
		d.stat.IncPut(uint64(len(value.Keys)))
	}

	for key, value := range taskInfo.AccountTask {
		startPut := time.Now()
		err = d.db.UpdateAccount([]byte(key), value)
		if err != nil {
			fmt.Println("update account err", err.Error())
		} else {
			d.updateAccount++
		}
		if d.db.GetMPTEngine() == VERSADBEngine {
			VersaDBAccPutLatency.Update(time.Since(startPut))
		} else {
			StateDBAccPutLatency.Update(time.Since(startPut))
		}
		d.keyCache.Add(key)
		d.stat.IncPut(1)
	}

	d.wDuration = time.Since(start)
	d.totalWriteCost += d.wDuration

	if d.db.GetMPTEngine() == VERSADBEngine {
		VeraDBPutTps.Update(int64(batchSize) / d.wDuration.Milliseconds())
	} else {
		stateDBPutTps.Update(int64(batchSize) / d.wDuration.Milliseconds())
	}

	if d.db.GetMPTEngine() == StateTrieEngine && d.db.GetFlattenDB() != nil {
		// simulate insert key to snap
		snapDB := d.db.GetFlattenDB()
		for key, value := range taskInfo.AccountTask {
			// add new account
			insertKey := common.BytesToHash([]byte(key))
			rawdb.WriteAccountSnapshot(snapDB, insertKey, value)
		}
		for key, value := range taskInfo.SmallStorageTask {
			accHash := common.BytesToHash([]byte(key))
			for i, k := range value.Keys {
				rawdb.WriteStorageSnapshot(snapDB, accHash, hashData([]byte(k)), []byte(value.Vals[i]))
			}
		}
		for key, value := range taskInfo.LargeStorageTask {
			accHash := common.BytesToHash([]byte(key))
			for i, k := range value.Keys {
				rawdb.WriteStorageSnapshot(snapDB, accHash, hashData([]byte(k)), []byte(value.Vals[i]))
			}
		}
	}

}

func (d *DBRunner) InitStorageTrie(
	taskInfo InitDBTask,
) {

	fmt.Println("init storage trie begin")
	var owners []common.Hash
	StorageInitSize := d.perfConfig.StorageTrieSize
	smallStorageSize := d.perfConfig.StorageTrieSize / 100
	var snapDB ethdb.KeyValueStore
	if d.db.GetMPTEngine() == StateTrieEngine && d.db.GetFlattenDB() != nil {
		// simulate insert key to snap
		snapDB = d.db.GetFlattenDB()
	}
	initTrieNum := 0

	for key, value := range taskInfo {
		start := time.Now()
		// add new storage
		d.db.AddStorage([]byte(key), value.Keys, value.Vals)

		// cache the inserted key for updating test
		if d.isLargeStorageTrie(key) {
			d.largeStorageCache[key] = value.Keys[StorageInitSize/2 : StorageInitSize/2+1000000]
		} else {
			d.ownerCache.Add(key)
			d.storageCache[key] = value.Keys[smallStorageSize/2 : smallStorageSize/2+100000]
		}
		owners = append(owners, common.BytesToHash([]byte(key)))

		if snapDB != nil {
			accHash := common.BytesToHash([]byte(key))
			for i, k := range value.Keys {
				rawdb.WriteStorageSnapshot(snapDB, accHash, hashData([]byte(k)), []byte(value.Vals[i]))
			}
		}
		// init 3 accounts to commit a block
		addresses, accounts := makeAccounts(3)
		for i := 0; i < len(addresses); i++ {
			initKey := string(crypto.Keccak256(addresses[i][:]))
			d.db.AddAccount(initKey, accounts[i])
			d.keyCache.Add(initKey)
			if d.db.GetMPTEngine() == StateTrieEngine && d.db.GetFlattenDB() != nil {
				rawdb.WriteAccountSnapshot(snapDB, common.BytesToHash([]byte(initKey)), accounts[i])
			}
		}

		if _, err := d.db.Commit(); err != nil {
			panic("failed to commit: " + err.Error())
		}
		initTrieNum++
		fmt.Println("init storage trie success", "cost time", time.Since(start).Seconds(), "s",
			"finish trie init num", initTrieNum)
	}

	// init the lock of each tree
	d.db.InitStorage(owners)
}

func (d *DBRunner) isLargeStorageTrie(owner string) bool {
	if owner == d.largeStorageTrie[0] || owner == d.largeStorageTrie[1] {
		return true
	}
	return false
}
