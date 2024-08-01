package main

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
)

// Database wraps access to tries and contract code.
type TrieDatabase interface {
	//OpenDB(string) TrieDatabase
	Put(key []byte, value []byte) error // insert single key value

	Get(key []byte) ([]byte, error)

	Delete(key []byte) error

	Commit() (common.Hash, error)

	Hash() common.Hash

	GetMPTEngine() string

	GetFlattenDB() ethdb.KeyValueStore
}

type TrieBatch interface {
	Put(key []byte, val []byte) error
	Del(key []byte) error
}
