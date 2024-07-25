package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/urfave/cli/v2"
)

const (
	PBSSEngine    = "pbss-mpt"
	VERSADBEngine = "versa-mpt"
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

// Run is the function to run the bsperftool command line tool
func main() {
	var config PerfConfig

	app := &cli.App{
		Name:    "perftool",
		Usage:   "A tool to perform state db benchmarking",
		Version: version,
		Commands: []*cli.Command{
			{
				Name:  "put",
				Usage: "Put random keys into the trie database",
				Action: func(c *cli.Context) error {
					run(c)
					return nil
				},
			},
		},
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "engine",
				Aliases:     []string{"e"},
				Usage:       "Engine to use",
				Destination: &config.Engine,
				Required:    true,
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
				Value:       3000,
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
				Value:       0.3,
				Destination: &config.DeleteRatio,
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

func run(c *cli.Context) error {

	var stateDB TrieDatabase
	engine := c.String("engine")
	if engine == PBSSEngine {
		fmt.Println("start to test", PBSSEngine)
		dir, _ := os.Getwd()
		stateDB = OpenPbssDB(filepath.Join(dir, "testdir"), types.EmptyRootHash)
		batchSize := c.Int64("bs")
		jobNum := c.Int("threads")
		keyRange := c.Uint64("key_range")
		maxValueSize := c.Uint64("max_value_size")
		minValueSize := c.Uint64("min_value_size")
		deleteRatio := c.Float64("delete_ratio")

		runner := NewRunner(stateDB, uint64(batchSize), jobNum, keyRange, minValueSize, maxValueSize, deleteRatio, 10)
		fmt.Println("begin to press test")
		runner.Run()
	}
	return nil
}
