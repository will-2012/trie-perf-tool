package main

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	ethTypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/trie/trienode"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/holiman/uint256"
	"golang.org/x/crypto/sha3"
)

type StateDBRunner struct {
	diskdb    ethdb.KeyValueStore
	triedb    *triedb.Database
	accTrie   *trie.StateTrie
	nodes     *trienode.MergedNodeSet
	stateTrie PbssStateTrie
}

func NewStateRunner(datadir string, root common.Hash) *StateDBRunner {
	triedb, _ := MakePBSSTrieDatabase(datadir)

	accTrie, err := trie.NewStateTrie(trie.StateTrieID(root), triedb)
	if err != nil {
		panic("create state trie err")
	}

	nodeSet := trienode.NewMergedNodeSet()
	if nodeSet == nil {
		panic("node set empty")
	}

	leveldb, err := rawdb.NewLevelDBDatabase("leveldb", 1000, 20000, "", false)
	if err != nil {
		panic("create leveldb err")
	}

	return &StateDBRunner{
		diskdb:  leveldb,
		triedb:  triedb,
		accTrie: accTrie,
		nodes:   nodeSet,
	}
}

func (v *StateDBRunner) AddAccount2(acckey string, acc *ethTypes.StateAccount) {
	val, _ := rlp.EncodeToBytes(acc)
	v.accTrie.MustUpdate([]byte(acckey), val)
}

func (v *StateDBRunner) AddAccount(acckey string, val []byte) error {
	v.accTrie.MustUpdate([]byte(acckey), val)
	v.AddSnapAccount(acckey, val)
	return nil
}

func (v *StateDBRunner) GetAccount(acckey string) ([]byte, error) {
	//	key := hashData([]byte(acckey))
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
	stRoot := v.makeStorageTrie(hashData(owner), keys, vals)
	acc := &ethTypes.StateAccount{Balance: uint256.NewInt(3),
		Root: stRoot, CodeHash: ethTypes.EmptyCodeHash.Bytes()}
	val, _ := rlp.EncodeToBytes(acc)
	v.AddAccount(string(owner), val)
	return nil
}

func (v *StateDBRunner) makeStorageTrie(owner common.Hash, keys []string, vals []string) common.Hash {
	id := trie.StorageTrieID(ethTypes.EmptyRootHash, owner, ethTypes.EmptyRootHash)
	stTrie, _ := trie.NewStateTrie(id, v.triedb)
	for i, k := range keys {
		stTrie.MustUpdate([]byte(k), []byte(vals[i]))
	}

	root, nodes := stTrie.Commit(false)
	if nodes != nil {
		v.nodes.Merge(nodes)
	}
	return root
}

func (s *StateDBRunner) Commit() (common.Hash, error) {
	root, nodes := s.accTrie.Commit(true)

	if nodes != nil {
		if err := s.nodes.Merge(nodes); err != nil {
			return ethTypes.EmptyRootHash, err
		}
	}
	s.triedb.Update(root, ethTypes.EmptyRootHash, 0, s.nodes, nil)
	s.accTrie, _ = trie.NewStateTrie(trie.TrieID(root), s.triedb)

	return root, nil
}

func (s *StateDBRunner) Hash() common.Hash {
	return s.accTrie.Hash()
}

func (s *StateDBRunner) GetMPTEngine() string {
	return StateTrieEngine
}

/*
func (v *VersaDBRunner) Hash() common.Hash {
	return  v.db.CalcRootHash()
}

func (p *VersaTrie) Put(key []byte, value []byte) error {
	return p.trie.Insert(key, value)
}

func (p *VersaTrie) Get(key []byte) ([]byte, error) {
	_, value, err := p.trie.Get(key)
	return value, err
}

func (p *VersaTrie) Delete(key []byte) error {
	return p.trie.Delete(key)
}

func (p *VersaTrie) Commit() (common.Hash, error) {
	hash, _, err := p.trie.Commit(0)
	return hash, err
}

func (p *VersaTrie) GetMPTEngine() string {
	return VERSADBEngine
}

func (p *VersaTrie) GetFlattenDB() ethdb.KeyValueStore {
	return nil
}


*/
