package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/urfave/cli/v2"
)

const (
	PbssRawTrieEngine = "pbss-mpt"
	VERSADBEngine     = "versa-mpt"
	StateTrieEngine   = "secure-trie"
)

// PerfConfig struct to hold command line arguments
type PerfConfig struct {
	Engine       string
	DataDir      string
	BatchSize    uint64
	NumJobs      int
	KeyRange     uint64
	MinValueSize uint64
	MaxValueSize uint64
	DeleteRatio  float64
}

const version = "1.0.0"

// Run is the function to runPerf the bsperftool command line tool
func main() {
	var config PerfConfig

	app := &cli.App{
		Name:    "perftool",
		Usage:   "A tool to perform state db benchmarking",
		Version: version,
		Commands: []*cli.Command{
			{
				Name:  "press-test",
				Usage: "Press random keys into the trie database",
				Action: func(c *cli.Context) error {
					runPerf(c)
					return nil
				},
			},
			{
				Name:  "verify-hash",
				Usage: "verify hash root of trie database by comparing",
				Action: func(c *cli.Context) error {
					verifyHash(c)
					return nil
				},
			},
		},

		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "engine",
				Aliases:     []string{"e"},
				Usage:       "Engine to use, engine can be pbss-mpt,versa-mpt or secure-trie",
				Destination: &config.Engine,
			},
			&cli.StringFlag{
				Name:        "datadir",
				Aliases:     []string{"d"},
				Usage:       "Data directory",
				Value:       "./dataset",
				Destination: &config.DataDir,
			},
			&cli.Uint64Flag{
				Name:        "bs",
				Aliases:     []string{"b"},
				Usage:       "Batch size",
				Value:       1000,
				Destination: &config.BatchSize,
			},
			&cli.IntFlag{
				Name:        "threads",
				Aliases:     []string{"t"},
				Usage:       "Number of threads",
				Value:       10,
				Destination: &config.NumJobs,
			},
			&cli.Uint64Flag{
				Name:        "key_range",
				Aliases:     []string{"r"},
				Usage:       "Key range",
				Value:       100000000,
				Destination: &config.KeyRange,
			},
			&cli.Uint64Flag{
				Name:        "min_value_size",
				Aliases:     []string{"m"},
				Usage:       "Minimum value size",
				Value:       300,
				Destination: &config.MinValueSize,
			},
			&cli.Uint64Flag{
				Name:        "max_value_size",
				Aliases:     []string{"M"},
				Usage:       "Maximum value size",
				Value:       300,
				Destination: &config.MaxValueSize,
			},
			&cli.Float64Flag{
				Name:        "delete_ratio",
				Aliases:     []string{"dr"},
				Usage:       "Delete ratio",
				Value:       0,
				Destination: &config.DeleteRatio,
			},
			&cli.DurationFlag{
				Name:    "runtime",
				Aliases: []string{"rt"},
				Value:   100 * time.Second,
				Usage:   "Duration to run the benchmark",
			},
		},
		Action: func(c *cli.Context) error {
			fmt.Printf("Running with config: %+v\n", config)
			return nil
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

func runPerf(c *cli.Context) error {
	var stateDB TrieDatabase
	engine := c.String("engine")
	if engine == PbssRawTrieEngine {
		fmt.Println("start to test trie:", PbssRawTrieEngine)
		dir, _ := os.Getwd()
		stateDB = OpenPbssDB(filepath.Join(dir, "pbss-dir"), types.EmptyRootHash)
	} else if engine == VERSADBEngine {
		fmt.Println("start to test trie:", VERSADBEngine)
		stateDB = OpenVersaTrie(0, nil)
	}
	runner := NewRunner(stateDB, parsePerfConfig(c), 100)
	ctx, cancel := context.WithTimeout(context.Background(), c.Duration("runtime"))
	defer cancel()

	runner.Run(ctx)
	return nil
}

func verifyHash(c *cli.Context) error {
	dir, _ := os.Getwd()
	secureTrie := OpenStateTrie(filepath.Join(dir, "test-dir"), types.EmptyRootHash)
	//versaTrie := OpenVersaTrie(0, nil)

	verifyer := NewVerifyer(nil, secureTrie, parsePerfConfig(c), 10)
	ctx, cancel := context.WithTimeout(context.Background(), c.Duration("runtime"))
	defer cancel()
	fmt.Println("begin to verify root hash, the batch size of block is", verifyer.perfConfig.BatchSize)
	verifyer.Run(ctx)
	return nil
}

func parsePerfConfig(c *cli.Context) PerfConfig {
	batchSize := c.Uint64("bs")
	threadNum := c.Int("threads")
	keyRange := c.Uint64("key_range")
	maxValueSize := c.Uint64("max_value_size")
	minValueSize := c.Uint64("min_value_size")
	deleteRatio := c.Float64("delete_ratio")
	return PerfConfig{
		BatchSize:    batchSize,
		NumJobs:      threadNum,
		KeyRange:     keyRange,
		MinValueSize: minValueSize,
		MaxValueSize: maxValueSize,
		DeleteRatio:  deleteRatio,
	}
}
