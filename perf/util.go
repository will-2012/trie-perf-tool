package main

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/holiman/uint256"
)

type Stat struct {
	ioStat     IOStat
	lastIoStat IOStat
}

type IOStat struct {
	get         uint64
	put         uint64
	getNotExist uint64
	delete      uint64
}

func NewStat() *Stat {
	return &Stat{
		ioStat: IOStat{
			get:         0,
			put:         0,
			getNotExist: 0,
			delete:      0,
		},
		lastIoStat: IOStat{
			get:         0,
			put:         0,
			getNotExist: 0,
			delete:      0,
		},
	}
}

func (s *Stat) CalcTpsAndOutput(delta time.Duration) string {
	get := atomic.LoadUint64(&s.ioStat.get)
	put := atomic.LoadUint64(&s.ioStat.put)
	del := atomic.LoadUint64(&s.ioStat.delete)
	getNotExist := atomic.LoadUint64(&s.ioStat.getNotExist)

	deltaF64 := delta.Seconds()

	getTps := float64(get-atomic.LoadUint64(&s.lastIoStat.get)) / deltaF64
	putTps := float64(put-atomic.LoadUint64(&s.lastIoStat.put)) / deltaF64
	deleteTps := float64(del-atomic.LoadUint64(&s.lastIoStat.delete)) / deltaF64
	getNotExistTps := float64(getNotExist-atomic.LoadUint64(&s.lastIoStat.getNotExist)) / deltaF64

	// keep io stat snapshot
	atomic.StoreUint64(&s.lastIoStat.get, get)
	atomic.StoreUint64(&s.lastIoStat.put, put)
	atomic.StoreUint64(&s.lastIoStat.delete, del)
	atomic.StoreUint64(&s.lastIoStat.getNotExist, getNotExist)

	return fmt.Sprintf(
		"tps: [get=%.2f, put=%.2f, delete=%.2f, get_not_exist=%.2f]",
		getTps, putTps, deleteTps, getNotExistTps,
	)
}

func (s *Stat) IncPut(num uint64) {
	atomic.AddUint64(&s.ioStat.put, num)
}

func (s *Stat) IncGet(num uint64) {
	atomic.AddUint64(&s.ioStat.get, num)
}

func (s *Stat) IncGetNotExist(num uint64) {
	atomic.AddUint64(&s.ioStat.getNotExist, num)
}

func (s *Stat) IncDelete(num uint64) {
	atomic.AddUint64(&s.ioStat.delete, num)
}

func makeAccounts(size int) (addresses [][20]byte, accounts [][]byte) {
	// Make the random benchmark deterministic
	random := rand.New(rand.NewSource(0))
	// Create a realistic account trie to hash
	addresses = make([][20]byte, size)
	for i := 0; i < len(addresses); i++ {
		data := make([]byte, 20)
		random.Read(data)
		copy(addresses[i][:], data)
	}
	accounts = make([][]byte, len(addresses))
	for i := 0; i < len(accounts); i++ {
		var (
			nonce = uint64(random.Int63())
			root  = types.EmptyRootHash
			code  = crypto.Keccak256(nil)
		)
		// The big.Rand function is not deterministic with regards to 64 vs 32 bit systems,
		// and will consume different amount of data from the rand source.
		//balance = new(big.Int).Rand(random, new(big.Int).Exp(common.Big2, common.Big256, nil))
		// Therefore, we instead just read via byte buffer
		numBytes := random.Uint32() % 33 // [0, 32] bytes
		balanceBytes := make([]byte, numBytes)
		random.Read(balanceBytes)
		balance := new(uint256.Int).SetBytes(balanceBytes)
		data, _ := rlp.EncodeToBytes(&types.StateAccount{Nonce: nonce, Balance: balance, Root: root, CodeHash: code})
		accounts[i] = data
	}
	return addresses, accounts
}

// randomFloat returns a random float64 between 0 and 1
func randomFloat() float64 {
	rand.Seed(time.Now().UnixNano())
	return rand.Float64()
}

type InsertedKeySet struct {
	mu      sync.Mutex
	items   []string
	maxSize int
	index   int
}

func NewFixedSizeSet(maxSize int) *InsertedKeySet {
	return &InsertedKeySet{
		items:   make([]string, 0, maxSize),
		maxSize: maxSize,
	}
}

func (s *InsertedKeySet) Add(item string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.items) < s.maxSize {
		s.items = append(s.items, item)
	} else {
		s.items[s.index] = item
		s.index = (s.index + 1) % s.maxSize
	}
}

func (s *InsertedKeySet) RandomItem() (string, bool) {
	//s.mu.Lock()
	//defer s.mu.Unlock()
	if len(s.items) == 0 {
		return "", false
	}
	randomIndex := rand.Intn(len(s.items))
	return s.items[randomIndex], true
}
