package main

import (
	"fmt"

	versa_db "versioned-state-database"

	"github.com/ethereum/go-ethereum/common"
	ethTypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/holiman/uint256"
)

type VersaDBRunner struct {
	db           *versa_db.VersaDB
	stateHandler versa_db.StateHandler
	rootTree     versa_db.TreeHandler
	version      int64
	stateRoot    common.Hash
	//	snapDB     ethdb.Database
}

func OpenVersaDB(version int64) *VersaDBRunner {
	db, err := versa_db.NewVersaDB(&versa_db.VersaDBConfig{
		FlushInterval:  200,
		MaxStatesInMem: 128,
	})
	if err != nil {
		panic(err)
	}
	stateHanlder, err := db.OpenState(version, ethTypes.EmptyRootHash, versa_db.S_COMMIT)
	if err != nil {
		panic(err)
	}
	rootTree, err := db.OpenTree(stateHanlder, version, common.Hash{}, ethTypes.EmptyRootHash)
	if err != nil {
		panic(err)
	}
	fmt.Println("init version db sucess")
	return &VersaDBRunner{
		db:           db,
		version:      version,
		stateRoot:    ethTypes.EmptyRootHash,
		rootTree:     rootTree,
		stateHandler: stateHanlder,
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
	stRoot := v.makeStorageTrie(hashData(owner), keys, vals)
	acc := &ethTypes.StateAccount{Balance: uint256.NewInt(3),
		Root: stRoot, CodeHash: ethTypes.EmptyCodeHash.Bytes()}
	val, _ := rlp.EncodeToBytes(acc)
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

	return hash
}

func (v *VersaDBRunner) Commit() (common.Hash, error) {
	hash, err := v.db.Commit(v.rootTree)
	if err != nil {
		fmt.Println("commit root tree err" + err.Error())
		return ethTypes.EmptyRootHash, err
	}
	err = v.db.Flush(v.stateHandler)
	if err != nil {
		fmt.Println("versa db flush err" + err.Error())
		return ethTypes.EmptyRootHash, err
	}
	err = v.db.CloseState(v.stateHandler)
	if err != nil {
		fmt.Println("versa db close handler" + err.Error())
		return ethTypes.EmptyRootHash, err
	}

	v.version++
	v.stateRoot = hash
	v.stateHandler, err = v.db.OpenState(v.version, hash, versa_db.S_COMMIT)
	if err != nil {
		log.Info("open state err" + err.Error())
		return ethTypes.EmptyRootHash, err
	}

	v.rootTree, err = v.db.OpenTree(v.stateHandler, v.version, common.Hash{}, v.stateRoot)
	if err != nil {
		log.Info("open root tree err" + err.Error())
		return ethTypes.EmptyRootHash, err
	}

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
