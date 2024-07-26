package main

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
)

type TrieVerifyer struct {
	verifyDB     TrieDatabase
	baseDB       TrieDatabase
	perfConfig   PerfConfig
	taskChan     chan map[string][]byte
	blockHeight  uint64
	successBatch uint64
	failBatch    uint64
}

func NewVerifyer(
	baseDB TrieDatabase,
	verifyDB TrieDatabase,
	config PerfConfig,
	taskBufferSize int, // Added a buffer size parameter for the task channel
) *TrieVerifyer {
	verifyer := &TrieVerifyer{
		verifyDB:   verifyDB,
		baseDB:     baseDB,
		perfConfig: config,
		taskChan:   make(chan map[string][]byte, taskBufferSize),
	}
	return verifyer
}

func (v *TrieVerifyer) Run(ctx context.Context) {
	defer close(v.taskChan)
	// Start task generation thread
	go generateTasks(ctx, v.taskChan, v.perfConfig.BatchSize)

	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case taskInfo := <-v.taskChan:
			v.compareHashRoot(taskInfo)
		case <-ticker.C:
			fmt.Printf(
				"[%s] verify In Progress, finish compare block %d \n",
				time.Now().Format(time.RFC3339Nano),
				v.successBatch)

		case <-ctx.Done():
			fmt.Println("Shutting down")
			return
		}
	}
}

func (v *TrieVerifyer) generateTasks(running *atomic.Bool) {
	for running.Load() {
		taskMap := make(map[string][]byte)
		address, acccounts := makeAccounts(int(v.perfConfig.BatchSize))
		for i := 0; i < len(address); i++ {
			taskMap[string(crypto.Keccak256(address[i][:]))] = acccounts[i]
		}
		v.taskChan <- taskMap
	}
}

func (v *TrieVerifyer) compareHashRoot(taskInfo map[string][]byte) {
	expectRoot := v.getRootHash(v.baseDB, taskInfo)
	verifyRoot := v.getRootHash(v.verifyDB, taskInfo)
	v.blockHeight++
	if expectRoot != verifyRoot {
		fmt.Printf("compare hash root not same, pbss root %v, versa root %v \n",
			expectRoot, verifyRoot)
		v.failBatch++
		panic("unexpect compare hash root")
	} else {
		v.successBatch++
	}
}

func (v *TrieVerifyer) getRootHash(db TrieDatabase, taskInfo map[string][]byte) common.Hash {
	// simulate insert and delete trie
	for key, value := range taskInfo {
		keyName := []byte(key)
		if randomFloat() > v.perfConfig.DeleteRatio {
			err := db.Put(keyName, value)
			if err != nil {
				fmt.Println("fail to insert key to trie", "key", string(keyName),
					"err", err.Error())
			}
		} else {
			err := db.Delete(keyName)
			if err != nil {
				fmt.Println("fail to delete key to trie", "key", string(keyName),
					"err", err.Error())
			}
		}
	}
	var hashRoot common.Hash
	if db.GetMPTEngine() == StateTrieEngine {
		hashRoot, _ = db.Commit()
	} else if db.GetMPTEngine() == VERSADBEngine {
		hashRoot = db.Hash()
	} else {
		hashRoot = types.EmptyRootHash
	}
	return hashRoot
}
