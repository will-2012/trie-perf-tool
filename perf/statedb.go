package main

import "github.com/ethereum/go-ethereum/common"

// Database wraps access to tries and contract code.
type TrieDatabase interface {
	//OpenDB(string) TrieDatabase
	Put(key []byte, value []byte) error // insert single key value

	Get(key []byte) ([]byte, error)

	Delete(key []byte) error
	//PutBatch(batch *TrieBatch) []error
	Commit() (common.Hash, error)

	Hash() common.Hash
}

type TrieBatch interface {
	Put(key []byte, val []byte) error
	Del(key []byte) error
}
