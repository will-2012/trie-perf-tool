package main

import (
	"versioned-state-database/store"
	versa_tree "versioned-state-database/tree"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

type VersaTrie struct {
	trie versa_tree.Tree
}

func (p *VersaTrie) OpenDB(dataDir string, root common.Hash) *VersaTrie {
	t := versa_tree.OpenTree(common.Hash{}, 0, nil, false, store.NewStore())
	return &VersaTrie{
		trie: t,
	}
}

func (p *VersaTrie) Hash() common.Hash {
	return p.trie.HashRoot()
}

func (p *VersaTrie) Put(key []byte, value []byte) error {
	if len(value) == 0 {
		panic("should not insert empty value")
	}
	return p.trie.Insert(key, value)
}

func (p *VersaTrie) Get(key []byte) ([]byte, error) {
	return p.trie.Get(key)
}

func (p *VersaTrie) Delete(key []byte) error {
	return p.trie.Delete(key)
}

func (p *VersaTrie) Commit(version uint64) (common.Hash, error) {
	//	hash, _, err := p.trie.Commit(version)

	return types.EmptyCodeHash, nil
}
