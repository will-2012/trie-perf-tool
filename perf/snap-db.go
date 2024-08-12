package main

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	ethTypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/trie/trienode"
	"github.com/ethereum/go-ethereum/trie/triestate"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/holiman/uint256"
	"golang.org/x/crypto/sha3"
)

type StateDBRunner struct {
	diskdb            ethdb.KeyValueStore
	triedb            *triedb.Database
	accTrie           *trie.StateTrie
	nodes             *trienode.MergedNodeSet
	stateTrie         PbssStateTrie
	parentRoot        common.Hash
	height            int64
	accountsOrigin    map[common.Address][]byte                 // The original value of mutated accounts in 'slim RLP' encoding
	storagesOrigin    map[common.Address]map[common.Hash][]byte // The original value of mutated slots in prefix-zero trimmed rlp forma
	stateRoot         common.Hash
	ownerStorageCache map[common.Hash]common.Hash
	lock              sync.RWMutex
	//ownerStorageTrieCache map[common.Hash]trie.StateTrie
}

func NewStateRunner(datadir string, root common.Hash) *StateDBRunner {
	triedb, _ := MakePBSSTrieDatabase(datadir)

	leveldb, err := rawdb.NewLevelDBDatabase("leveldb", 1000, 20000, "", false)
	if err != nil {
		panic("create leveldb err")
	}

	rootBytes, err := leveldb.Get(InitFinishRoot)
	if err == nil && rootBytes != nil {
		root = common.BytesToHash(rootBytes)
	}

	accTrie, err := trie.NewStateTrie(trie.StateTrieID(root), triedb)
	if err != nil {
		panic("create state trie err")
	}

	nodeSet := trienode.NewMergedNodeSet()
	if nodeSet == nil {
		panic("node set empty")
	}

	s := &StateDBRunner{
		diskdb:            leveldb,
		triedb:            triedb,
		accTrie:           accTrie,
		nodes:             nodeSet,
		height:            0,
		parentRoot:        ethTypes.EmptyRootHash,
		accountsOrigin:    make(map[common.Address][]byte),
		storagesOrigin:    make(map[common.Address]map[common.Hash][]byte),
		ownerStorageCache: make(map[common.Hash]common.Hash),
	}

	// Initialize with 2 random elements
	s.initializeAccountsOrigin(1)
	s.initializeStoragesOrigin(1)

	return s
}

// Initialize accountsOrigin with n random elements
func (v *StateDBRunner) initializeAccountsOrigin(n int) {
	for i := 0; i < n; i++ {
		addr := common.BytesToAddress(randBytes(20))
		data := randBytes(32)
		v.accountsOrigin[addr] = data
	}
}

// Initialize storagesOrigin with n random elements
func (v *StateDBRunner) initializeStoragesOrigin(n int) {
	for i := 0; i < n; i++ {
		addr := common.BytesToAddress(randBytes(20))
		v.storagesOrigin[addr] = make(map[common.Hash][]byte)
		for j := 0; j < 2; j++ { // Assume each account has 2 storage items
			key := common.BytesToHash(randBytes(32))
			val := randBytes(6)
			v.storagesOrigin[addr][key] = val
		}
	}
}

// Helper function to generate random bytes
func randBytes(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	return b
}

func (v *StateDBRunner) AddAccount(acckey string, val []byte) error {
	v.accTrie.MustUpdate([]byte(acckey), val)
	return nil
}

func (v *StateDBRunner) GetAccount(acckey string) ([]byte, error) {
	return rawdb.ReadAccountSnapshot(v.diskdb, common.BytesToHash([]byte(acckey))), nil
}

func (v *StateDBRunner) AddSnapAccount(acckey string, val []byte) {
	key := common.BytesToHash([]byte(acckey))
	rawdb.WriteAccountSnapshot(v.diskdb, key, val)
}

func hashData(input []byte) common.Hash {
	var hasher = sha3.NewLegacyKeccak256()
	var hash common.Hash
	hasher.Reset()
	hasher.Write(input)
	hasher.Sum(hash[:0])
	return hash
}

func (v *StateDBRunner) AddStorage(owner []byte, keys []string, vals []string) error {
	stRoot, err := v.makeStorageTrie(common.BytesToHash(owner), keys, vals)
	if err != nil {
		return err
	}
	acc := &ethTypes.StateAccount{Balance: uint256.NewInt(3),
		Root: stRoot, CodeHash: ethTypes.EmptyCodeHash.Bytes()}
	val, _ := rlp.EncodeToBytes(acc)
	v.AddAccount(string(owner), val)
	return nil
}

func (v *StateDBRunner) makeStorageTrie(owner common.Hash, keys []string, vals []string) (common.Hash, error) {
	id := trie.StorageTrieID(v.stateRoot, owner, ethTypes.EmptyRootHash)
	stTrie, _ := trie.NewStateTrie(id, v.triedb)
	for i, k := range keys {
		stTrie.MustUpdate([]byte(k), []byte(vals[i]))
	}

	root, nodes, err := stTrie.Commit(true)
	if err != nil {
		return ethTypes.EmptyRootHash, err
	}
	if nodes != nil {
		v.nodes.Merge(nodes)
	}
	v.lock.Lock()
	v.ownerStorageCache[owner] = root
	v.lock.Unlock()

	return root, nil
}

func (s *StateDBRunner) GetStorage(owner []byte, key []byte) ([]byte, error) {
	return rawdb.ReadStorageSnapshot(s.diskdb, common.BytesToHash(owner), hashData(key)), nil
}

// UpdateStorage  update batch k,v of storage trie
func (s *StateDBRunner) UpdateStorage(owner []byte, keys []string, vals []string) error {
	var err error
	ownerHash := common.BytesToHash(owner)
	// try to get version and root from cache first
	s.lock.RLock()
	root, exist := s.ownerStorageCache[ownerHash]
	s.lock.RUnlock()
	if !exist {
		return fmt.Errorf("fail to get storage trie root in cache")
	}

	id := trie.StorageTrieID(s.stateRoot, ownerHash, root)
	stTrie, _ := trie.NewStateTrie(id, s.triedb)
	for i, k := range keys {
		stTrie.MustUpdate([]byte(k), []byte(vals[i]))
	}

	// update batch storage trie
	for i := 0; i < len(keys) && i < len(vals); i++ {
		stTrie.MustUpdate([]byte(keys[i]), []byte(vals[i]))
	}

	root, nodes, err := stTrie.Commit(true)
	if err != nil {
		return err
	}
	if nodes != nil {
		s.nodes.Merge(nodes)
	}

	s.lock.Lock()
	s.ownerStorageCache[ownerHash] = root
	s.lock.Unlock()

	return err
}

func (s *StateDBRunner) UpdateAccount(key, value []byte) error {
	originValue, err := s.GetAccount(string(key))
	if err != nil {
		return err
	}
	if bytes.Equal(originValue, value) {
		return nil
	}
	s.accTrie.MustUpdate(key, value)
	return nil
}

func (s *StateDBRunner) InitStorage(owners []common.Hash) {
	return
}

func (s *StateDBRunner) Commit() (common.Hash, error) {
	root, nodes, err := s.accTrie.Commit(false)
	if err != nil {
		return ethTypes.EmptyRootHash, err
	}
	if nodes != nil {
		if err := s.nodes.Merge(nodes); err != nil {
			return ethTypes.EmptyRootHash, err
		}
	}

	set := triestate.New(s.accountsOrigin, s.storagesOrigin, nil)
	err = s.triedb.Update(root, s.parentRoot, uint64(s.height), s.nodes, set)
	if err != nil {
		fmt.Println("trie update err", err.Error())
	}

	s.accTrie, _ = trie.NewStateTrie(trie.TrieID(root), s.triedb)
	s.parentRoot = root
	s.stateRoot = root
	s.height++
	s.nodes = trienode.NewMergedNodeSet()
	s.MarkInitRoot(root)
	return root, nil
}

func (s *StateDBRunner) Hash() common.Hash {
	return s.accTrie.Hash()
}

func (s *StateDBRunner) GetMPTEngine() string {
	return StateTrieEngine
}

func (p *StateDBRunner) GetFlattenDB() ethdb.KeyValueStore {
	return p.diskdb
}

func (s *StateDBRunner) MarkInitRoot(hash common.Hash) {
	s.diskdb.Put(InitFinishRoot, hash.Bytes())
}
