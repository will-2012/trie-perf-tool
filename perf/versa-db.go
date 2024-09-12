package main

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	versaDB "github.com/bnb-chain/versioned-state-database"
	"github.com/ethereum/go-ethereum/common"
	ethTypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/holiman/uint256"
)

type VersaDBRunner struct {
	db           *versaDB.VersaDB
	stateHandler versaDB.StateHandler
	rootTree     versaDB.TreeHandler
	version      int64
	stateRoot    common.Hash

	ownerHandlerCache map[common.Hash]versaDB.TreeHandler
	ownerStorageCache map[common.Hash]StorageCache
	handlerLock       sync.RWMutex
	lock              sync.RWMutex
	treeOpenLocks     map[common.Hash]*sync.Mutex
	diskVersion       int64
	//	storageOwners     []common.Hash // Global slice for storage owners
}

type StorageCache struct {
	version int64
	stRoot  common.Hash
}

func OpenVersaDB(path string, version int64) *VersaDBRunner {
	db, err := versaDB.NewVersaDB(path, &versaDB.VersaDBConfig{
		FlushInterval:  1000,
		MaxStatesInMem: 128,
	})
	if err != nil {
		panic(err)
	}
	initHash := ethTypes.EmptyRootHash

	version, initHash = db.LatestStoreDiskVersionInfo()
	if version >= 0 {
		fmt.Printf("init from existed db, version %d, root hash %v \n", version, initHash)
	}

	stateHanlder, err := db.OpenState(version, initHash, versaDB.S_COMMIT)
	if err != nil {
		panic(err)
	}

	rootTree, err := db.OpenTree(stateHanlder, version, common.Hash{}, initHash)
	if err != nil {
		panic(err)
	}
	fmt.Println("init version db success")

	return &VersaDBRunner{
		db:                db,
		version:           version,
		stateRoot:         initHash,
		rootTree:          rootTree,
		stateHandler:      stateHanlder,
		ownerHandlerCache: make(map[common.Hash]versaDB.TreeHandler),
		ownerStorageCache: make(map[common.Hash]StorageCache),
		diskVersion:       version,
	}
}

func (v *VersaDBRunner) AddAccount(acckey string, val []byte) error {
	return v.db.Put(v.rootTree, []byte(acckey), val)
}

func (v *VersaDBRunner) GetAccount(acckey string) ([]byte, error) {
	_, val, err := v.db.Get(v.rootTree, []byte(acckey))
	if err != nil {
		return nil, err
	}
	return val, nil
}

func (v *VersaDBRunner) AddStorage(owner []byte, keys []string, vals []string) error {
	ownerHash := common.BytesToHash(owner)
	stRoot := v.makeStorageTrie(ownerHash, keys, vals)
	//random := rand.New(rand.NewSource(0))
	acc := &ethTypes.StateAccount{Nonce: uint64(2), Balance: uint256.NewInt(3),
		Root: stRoot, CodeHash: generateCodeHash(owner).Bytes()}
	val, err := rlp.EncodeToBytes(acc)
	if err != nil {
		fmt.Println("encode acc err", err.Error())
	}

	return v.AddAccount(string(owner), val)
}

func (v *VersaDBRunner) makeStorageTrie(owner common.Hash, keys []string, vals []string) common.Hash {
	tHandler, err := v.db.OpenTree(v.stateHandler, v.version, owner, ethTypes.EmptyRootHash)
	if err != nil {
		panic(fmt.Sprintf("failed to open tree, version: %d, owner: %d, err: %s", version, owner, err.Error()))
	}
	for i, k := range keys {
		v.db.Put(tHandler, []byte(k), []byte(vals[i]))
	}

	hash, err := v.db.Commit(tHandler)
	if err != nil {
		panic(fmt.Sprintf("failed to commit tree, version: %d, owner: %d, err: %s", version, owner, err.Error()))
	}

	v.lock.Lock()
	v.ownerStorageCache[owner] = StorageCache{
		version: v.version + 1,
		stRoot:  hash,
	}
	v.lock.Unlock()
	return hash
}

func (v *VersaDBRunner) InitStorage(owners []common.Hash, trieNum int) {
	v.treeOpenLocks = make(map[common.Hash]*sync.Mutex, trieNum)

	// Initialize ownerLocks using the global storageOwners slice
	for i := 0; i < trieNum; i++ {
		fmt.Println("init lock of owner:", owners[i])
		v.treeOpenLocks[owners[i]] = &sync.Mutex{}
	}

}

// UpdateStorage  update batch k,v of storage trie
func (v *VersaDBRunner) UpdateStorage(owner []byte, keys []string, values []string) (common.Hash, error) {
	var err error
	ownerHash := common.BytesToHash(owner)
	var tHandler versaDB.TreeHandler

	v.handlerLock.RLock()
	tHandler, found := v.ownerHandlerCache[ownerHash]
	v.handlerLock.RUnlock()
	if !found {
		// try to get version and root from cache first
		v.lock.RLock()
		cache, exist := v.ownerStorageCache[ownerHash]
		v.lock.RUnlock()
		if !exist {
			// should always exist in cache
			fmt.Println("fail to version and root from cache")
		} else {
			versionNum := cache.version
			stRoot := cache.stRoot
			tHandler, err = v.db.OpenTree(v.stateHandler, versionNum, ownerHash, stRoot)
			if err != nil {
				return ethTypes.EmptyRootHash, fmt.Errorf("failed to open tree, version: %d, owner: %s, block height %d, err: %v", versionNum,
					ownerHash.String(), v.version, err.Error())
			}
		}
	}
	for i := 0; i < len(keys) && i < len(values); i++ {
		err = v.db.Put(tHandler, []byte(keys[i]), []byte(values[i]))
		if err != nil {
			fmt.Println("failed to read key using caching tree hanlder, err:", err.Error())
		}
	}

	hash, err := v.db.Commit(tHandler)
	if err != nil {
		panic(fmt.Sprintf("failed to commit tree, version: %d, owner: %d, err: %s", version, owner, err.Error()))
	}

	acc := &ethTypes.StateAccount{Nonce: uint64(2), Balance: uint256.NewInt(3),
		Root: hash, CodeHash: generateCodeHash(owner).Bytes()}
	val, err := rlp.EncodeToBytes(acc)
	if err != nil {
		fmt.Println("encode acc err", err.Error())
	}

	err = v.UpdateAccount(owner, val)
	if err != nil {
		panic(fmt.Sprintf("failed add account of owner version: %d, owner: %d, err: %s", version, owner, err.Error()))
	}

	// handler is unuseful after commit
	v.handlerLock.Lock()
	delete(v.ownerHandlerCache, ownerHash)
	v.handlerLock.Unlock()

	v.lock.Lock()
	v.ownerStorageCache[ownerHash] = StorageCache{
		version: v.version + 1,
		stRoot:  hash,
	}
	// update the cache for read
	v.lock.Unlock()

	return hash, nil
}

func (v *VersaDBRunner) UpdateAccount(key, value []byte) error {
	_, originValue, err := v.db.Get(v.rootTree, key)
	if err != nil {
		return err
	}
	if len(originValue) == 0 {
		return fmt.Errorf("get account nil when update")
	}
	if bytes.Equal(originValue, value) {
		fmt.Println("update account no value update")
		return nil
	}
	return v.db.Put(v.rootTree, key, value)
}

func (v *VersaDBRunner) GetStorage(owner []byte, key []byte) ([]byte, error) {
	ownerHash := common.BytesToHash(owner)
	v.handlerLock.RLock()
	tHandler, found := v.ownerHandlerCache[ownerHash]
	v.handlerLock.RUnlock()
	if !found {
		var stRoot common.Hash
		var versionNum int64
		var encodedData []byte
		var err error
		// try to get version and root from cache first
		v.lock.RLock()
		cache, exist := v.ownerStorageCache[ownerHash]
		v.lock.RUnlock()
		if !exist {
			versionNum, encodedData, err = v.db.Get(v.rootTree, owner)
			if err != nil {
				return nil, err
			}
			//	fmt.Println("get account len:", len(encodedData), "version", versionNum, "owner: ", ownerHash)
			account := new(ethTypes.StateAccount)
			err = rlp.DecodeBytes(encodedData, account)
			if err != nil {
				fmt.Printf("Failed to decode RLP %v, db get CA account %s, version %d, val len:%d, versrion2 %d\n",
					err, common.BytesToHash(owner).String(),
					v.version, len(encodedData), versionNum)
				return nil, err
			}
			stRoot = account.Root
			v.lock.Lock()
			v.ownerStorageCache[ownerHash] = StorageCache{
				version: versionNum,
				stRoot:  stRoot,
			}
			// update the cache for read
			v.lock.Unlock()

		} else {
			versionNum = cache.version
			stRoot = cache.stRoot
		}

		// Check if the owner is in the opened
		handler, err := v.tryGetTreeLock(ownerHash, stRoot, versionNum)
		if err != nil {
			return nil, err
		}
		tHandler = *handler
	}
	_, val, err := v.db.Get(tHandler, key)
	if err != nil {
		if found {
			fmt.Println("failed to read key using caching tree hanlder, err:", err.Error())
		} else {
			fmt.Println("failed to open tree and read key, err:", err.Error())
		}
	}
	return val, err
}

func (v *VersaDBRunner) tryGetTreeLock(ownerHash, stRoot common.Hash, versionNum int64) (*versaDB.TreeHandler, error) {
	var tHandler versaDB.TreeHandler
	var found bool
	var err error
	v.handlerLock.RLock()
	tHandler, found = v.ownerHandlerCache[ownerHash]
	v.handlerLock.RUnlock()
	if found {
		return &tHandler, nil
	}
	getOpenTreeLock := v.treeOpenLocks[ownerHash].TryLock()
	if !getOpenTreeLock {
		fmt.Println("storage trie is opening :", ownerHash.String(), "version", versionNum,
			"try to get the handler for 3 times")
		start := time.Now()
		num := 0
		for i := 0; i < 10; i++ {
			num++
			v.handlerLock.RLock()
			tHandler, found = v.ownerHandlerCache[ownerHash]
			v.handlerLock.RUnlock()
			if found {
				fmt.Println("success to get the handler after waiting, owner hash", ownerHash, "version",
					versionNum)
				return &tHandler, nil
			}
			time.Sleep(100 * time.Microsecond)
		}
		fmt.Println("wait fail", "cost time", time.Since(start).Microseconds(), "us", "wait time", num,
			"owner", ownerHash, "height", v.version)
		// open tree should cost less than 6000 us
		panic("fail to get the handler after sleeping:" + ownerHash.String() + string(v.version))
	} else {
		tHandler, err = v.db.OpenTree(v.stateHandler, versionNum, ownerHash, stRoot)
		if err != nil {
			v.treeOpenLocks[ownerHash].Unlock()
			return nil, fmt.Errorf("failed to open tree, version: %d, owner: %s, block height %d, err: %v", versionNum,
				ownerHash.String(), v.version, err.Error())
		}

		// update the handler cache for next read
		v.handlerLock.Lock()
		v.ownerHandlerCache[ownerHash] = tHandler
		v.handlerLock.Unlock()
		v.treeOpenLocks[ownerHash].Unlock()
	}
	return &tHandler, nil
}

func (v *VersaDBRunner) Commit() (common.Hash, error) {
	hash, err := v.db.Commit(v.rootTree)
	if err != nil {
		fmt.Printf("commit root tree err:%v, version %d \n", err, v.version)
		return ethTypes.EmptyRootHash, err
	}
	err = v.db.Flush(v.stateHandler)
	if err != nil {
		fmt.Printf("versa db flush err:%v, version %d \n", err, v.version)
		return ethTypes.EmptyRootHash, err
	}
	err = v.db.CloseState(v.stateHandler)
	if err != nil {
		fmt.Println("versa db close handler" + err.Error())
		return ethTypes.EmptyRootHash, err
	}

	v.version++
	v.stateRoot = hash
	v.stateHandler, err = v.db.OpenState(v.version, hash, versaDB.S_COMMIT)
	if err != nil {
		log.Info("open state err" + err.Error())
		return ethTypes.EmptyRootHash, err
	}

	v.rootTree, err = v.db.OpenTree(v.stateHandler, v.version, common.Hash{}, v.stateRoot)
	if err != nil {
		log.Info("open root tree err" + err.Error())
		return ethTypes.EmptyRootHash, err
	}

	v.ownerHandlerCache = make(map[common.Hash]versaDB.TreeHandler)
	return hash, nil
}

func (v *VersaDBRunner) Hash() common.Hash {
	hash, err := v.db.CalcRootHash(v.rootTree)
	if err != nil {
		panic("cal hash err" + err.Error())
	}
	return hash
}

func (v *VersaDBRunner) GetMPTEngine() string {
	return VERSADBEngine
}

func (v *VersaDBRunner) GetFlattenDB() ethdb.KeyValueStore {
	return nil
}

func (v *VersaDBRunner) RepairSnap(owners []string) {
	return
}

func (v *VersaDBRunner) GetVersion() int64 {
	return v.diskVersion
}
