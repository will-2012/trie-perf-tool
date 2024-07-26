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
  -b, --bs <BATCH_SIZE>                  [default: 1000]
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
- `-dr, --delete_ratio` delete ratio in a batch
- `-r, --key_range` the random key range, `0-100000000` by default
- `-m, --min_value_size` the minimum random value size
- `-M, --max_value_size` the maximum random value size


the result shown below:

```bash
// press test for pbss-mpt 
$ ./build/perftool -engine pbss-mpt -b 1000 -dr 0.1 -threads 1 press-test 

start to test pbss-mpt
init trie finish, begin to press kv
[2024-07-26T13:06:32.149912+08:00] Perf In Progress tps: [get=11867.61, put=10642.14, delete=1225.47, get_not_exist=0.00], block height=65 elapsed: [rw=41.473787ms, commit=21.480795ms, cal hash=1.439519ms]
[2024-07-26T13:06:35.15027+08:00] Perf In Progress tps: [get=19333.31, put=17397.98, delete=1935.33, get_not_exist=0.00], block height=123 elapsed: [rw=32.107913ms, commit=19.507491ms, cal hash=1.347806ms]
[2024-07-26T13:06:38.072403+08:00] Perf In Progress tps: [get=17795.91, put=16029.33, delete=1766.59, get_not_exist=0.00], block height=175 elapsed: [rw=37.528568ms, commit=31.796649ms, cal hash=2.746411ms]

// verify the root hash of versa-mpt and pbss-mpt, run time is 1 minute

$ ./build/perftool -b 100  -dr 0 -runtime 1m verify-hash
begin to verify root hash, the batch size of block is 100
[2024-07-26T12:41:40.731314+08:00] verify In Progress, finish compare block 1355
[2024-07-26T12:41:43.735671+08:00] verify In Progress, finish compare block 2747
[2024-07-26T12:41:46.731497+08:00] verify In Progress, finish compare block 4105
[2024-07-26T12:41:49.73112+08:00] verify In Progress, finish compare block 5485
```
