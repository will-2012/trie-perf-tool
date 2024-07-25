# trie-perf-tool
A perf tool for mpt engine press test

## Build
```bash
make 
```

## Usage

```bash
$ ./build/perftool -h       
Usage: bsperftool [OPTIONS] --engine <ENGINE>

Options:
  -e, --engine <ENGINE>                  
  -d, --datadir <DATADIR>                [default: ./dataset]
  -b, --bs <BATCH_SIZE>                  [default: 3000]
  -t, --threads <THREAD>              [default: 10]
  -m, --min_value_size <MIN_VALUE_SIZE>  [default: 300]
  -M, --max_value_size <MAX_VALUE_SIZE>  [default: 300]
  -d, --delete_ratio <DELETE_RATIO>      [default: 0.2]
  -h, --help                             Print help
  -V, --version                          Print version
```

- `-e, --engine` db engine type. Now support `memorydb` and `firewood`
- `-d, --datadir` the data directory of the database
- `-b, --bs` the number of keys read and write within a block
- `-t, --threads` concurrency thread number
- `-d, --delete_ratio` delete ratio in a batch
- `-r, --key_range` the random key range, `0-100000000` by default
- `-m, --min_value_size` the minimum random value size
- `-M, --max_value_size` the maximum random value size


the result shown below:

```bash
$ ./build/perftool -engine pbss-mpt -b 1000 -threads 1 put
```
