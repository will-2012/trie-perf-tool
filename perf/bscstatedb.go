package main

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state/snapshot"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/ethereum/go-ethereum/triedb/pathdb"
)

type BscStateDBRunner struct {
	chaindb  ethdb.Database
	snaps    *snapshot.Tree
	tries    map[common.Hash]trie.Trie
	accounts map[common.Address][]byte
	storages map[common.Address]map[common.Hash][]byte
}

func NewBSCStateRunner(datadir string, root common.Hash) StateDatabase {
	db, err := rawdb.Open(rawdb.OpenOptions{
		Type:              "pebble",
		Directory:         "./bscstate",
		AncientsDirectory: "./bscstate/ancient",
		Namespace:         "eth/db/chaindata/",
		Cache:             80000,
		Handles:           65536,
		ReadOnly:          false,
	})
	if err != nil {
		panic("open rawdb error" + err.Error())
	}
	scheme, err := rawdb.ParseStateScheme(rawdb.PathScheme, db)
	if err != nil {
		panic("parse state scheme error" + err.Error())
	}
	// Try to recover offline state pruning only in hash-based.
	if scheme == rawdb.HashScheme {
		panic("not hash base scheme")
	}
	// open trie db
	config := &triedb.Config{
		PathDB: pathdb.Defaults,
	}
	tdb := triedb.NewDatabase(db, config)

	// open snaps
	snapconfig := snapshot.Config{
		CacheSize:  256,
		Recovery:   false,
		NoBuild:    true,
		AsyncBuild: false,
	}
	snapdb, _ := snapshot.New(snapconfig, db, tdb, root)

	return &BscStateDBRunner{
		chaindb: db,
		snaps:   snapdb,
	}
}

// AddAccount implements StateDatabase.
func (b *BscStateDBRunner) AddAccount(key string, value []byte) error {
	b.stateobjects[common.BytesToAddress([]byte(key))] = value
}

// AddStorage implements StateDatabase.
func (b *BscStateDBRunner) AddStorage(owner []byte, keys []string, vals []string) error {
	panic("unimplemented")
}

// Commit implements StateDatabase.
func (b *BscStateDBRunner) Commit() (common.Hash, error) {
	panic("unimplemented")
}

// GetAccount implements StateDatabase.
func (b *BscStateDBRunner) GetAccount(string) ([]byte, error) {
	panic("unimplemented")
}

// GetFlattenDB implements StateDatabase.
func (b *BscStateDBRunner) GetFlattenDB() ethdb.KeyValueStore {
	panic("unimplemented")
}

// GetMPTEngine implements StateDatabase.
func (b *BscStateDBRunner) GetMPTEngine() string {
	panic("unimplemented")
}

// Hash implements StateDatabase.
func (b *BscStateDBRunner) Hash() common.Hash {
	panic("unimplemented")
}
