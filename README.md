# trie-perf-tool
A perf tool for mpt engine press test

## Build



## Usage

```bash
$ ./bsperftool -h       
Usage: bsperftool [OPTIONS] --engine <ENGINE>

Options:
  -e, --engine <ENGINE>                  
  -d, --datadir <DATADIR>                [default: ./dataset]
  -b, --bs <BATCH_SIZE>                  [default: 3000]
  -j, --num_jobs <NUM_JOBS>              [default: 10]
  -r, --key_range <KEY_RANGE>            [default: 100000000]
  -m, --min_value_size <MIN_VALUE_SIZE>  [default: 300]
  -M, --max_value_size <MAX_VALUE_SIZE>  [default: 300]
  -d, --delete_ratio <DELETE_RATIO>      [default: 0.2]
  -h, --help                             Print help
  -V, --version                          Print version
```

- `-e, --engine` db engine type. Now support `memorydb` and `firewood`
- `-d, --datadir` the data directory of the database
- `-b, --bs` the number of keys read and write within a block
- `-j, --num_jobs` concurrency jobs number
- `-d, --delete_ratio` delete ratio in a batch
- `-r, --key_range` the random key range, `0-100000000` by default
- `-m, --min_value_size` the minimum random value size
- `-M, --max_value_size` the maximum random value size


the result shown below:

```bash
$ ./bsperftool --engine='memorydb' -j3000 
Open memory MPT success
[2024-05-30T11:29:53.212Z] Perf In Progress. tps: [get=72721.41, put=72718.50, delete=2.91, get_not_exist=72694.26], elapsed: [rw=25.57625ms, commit=28.499833ms]
[2024-05-30T11:29:54.221Z] Perf In Progress. tps: [get=56457.03, put=56439.21, delete=17.83, get_not_exist=56390.67], elapsed: [rw=25.499084ms, commit=29.518958ms]
[2024-05-30T11:29:55.267Z] Perf In Progress. tps: [get=51622.09, put=51609.66, delete=12.43, get_not_exist=51538.92], elapsed: [rw=26.081166ms, commit=31.5205ms]
[2024-05-30T11:29:56.321Z] Perf In Progress. tps: [get=48412.25, put=48387.57, delete=24.68, get_not_exist=48306.88], elapsed: [rw=27.274625ms, commit=35.046875ms]
[2024-05-30T11:29:57.325Z] Perf In Progress. tps: [get=44831.60, put=44807.69, delete=23.91, get_not_exist=44708.06], elapsed: [rw=34.478ms, commit=34.62175ms]
```
