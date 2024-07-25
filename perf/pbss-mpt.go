package main

import (
	"fmt"
	"path/filepath"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	bsctrie "github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/trie/trienode"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/ethereum/go-ethereum/triedb/pathdb"
)

type PbssTrie struct {
	trie  *bsctrie.Trie
	db    *triedb.Database
	nodes *trienode.MergedNodeSet
}

func OpenPbssDB(dataDir string, root common.Hash) *PbssTrie {
	triedb, _ := MakePBSSTrieDatabase(dataDir)

	t, err := bsctrie.New(bsctrie.StateTrieID(root), triedb)
	if err != nil {
		panic("create state trie err")
	}

	nodeSet := trienode.NewMergedNodeSet()
	if nodeSet == nil {
		panic("node set empty")
	}
	return &PbssTrie{
		trie:  t,
		db:    triedb,
		nodes: nodeSet,
	}
}

func (p *PbssTrie) Commit() (common.Hash, error) {
	root, nodes, err := p.trie.Commit(true)
	if err != nil {
		fmt.Println("commit err", err)
		return types.EmptyRootHash, err
	}

	if nodes != nil {
		if err = p.nodes.Merge(nodes); err != nil {
			return types.EmptyRootHash, err
		}
	}

	p.db.Update(root, types.EmptyRootHash, 0, p.nodes, nil)
	p.trie, _ = bsctrie.New(bsctrie.TrieID(root), p.db)

	return root, err
}

func (p *PbssTrie) Hash() common.Hash {
	return p.trie.Hash()
}

func (p *PbssTrie) Put(key []byte, value []byte) error {
	if len(value) == 0 {
		panic("should not insert empty value")
	}
	return p.trie.Update(key, value)
}

func (p *PbssTrie) Get(key []byte) ([]byte, error) {
	return p.trie.Get(key)
}

func (p *PbssTrie) Delete(key []byte) error {
	return p.trie.Delete(key)
}

func createChainDataBase(datadir string) (ethdb.Database, error) {
	db, err := OpenDatabaseWithFreezer("geth", 10000, 10000, "", "chaindata",
		datadir)
	if err != nil {
		return nil, err
	}
	return db, err
}

// MakePBSSTrieDatabase constructs a trie database based on the configured scheme.
func MakePBSSTrieDatabase(datadir string) (*triedb.Database, error) {
	diskdb, err := createChainDataBase(datadir)
	if err != nil {
		return nil, err
	}
	config := &triedb.Config{
		PathDB: pathdb.Defaults,
	}

	//config.PathDB.JournalFilePath = fmt.Sprintf("%s/%s", stack.ResolvePath("chaindata"), eth.JournalFileName)
	return triedb.NewDatabase(diskdb, config), nil
}

func OpenDatabaseWithFreezer(name string, cache, handles int, ancient, namespace, datadir string) (ethdb.Database, error) {
	db, err := rawdb.Open(rawdb.OpenOptions{
		Type:              "pebble",
		Directory:         filepath.Join(datadir, name),
		AncientsDirectory: filepath.Join(filepath.Join(datadir, name), "ancient"),
		Namespace:         namespace,
		Cache:             cache,
		Handles:           handles,
		ReadOnly:          false,
	})
	return db, err
}
