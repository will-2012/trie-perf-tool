package main

import (
	"versioned-state-database/store"
	versa_tree "versioned-state-database/tree"

	"github.com/ethereum/go-ethereum/common"
)

type VersaTrie struct {
	trie versa_tree.Tree
}

func OpenVersaTrie(version uint64, rootHash []byte) *VersaTrie {
	t := versa_tree.OpenTree(common.Hash{}, version, rootHash, false, store.NewStore())
	return &VersaTrie{
		trie: t,
	}
}

func (p *VersaTrie) Hash() common.Hash {
	return p.trie.HashRoot()
}

func (p *VersaTrie) Put(key []byte, value []byte) error {
	return p.trie.Insert(key, value)
}

func (p *VersaTrie) Get(key []byte) ([]byte, error) {
	return p.trie.Get(key)
}

func (p *VersaTrie) Delete(key []byte) error {
	return p.trie.Delete(key)
}

func (p *VersaTrie) Commit() (common.Hash, error) {
	hash, _, _, err := p.trie.Commit(0)
	return hash, err
}

func (p *VersaTrie) GetMPTEngine() string {
	return VERSADBEngine
}
