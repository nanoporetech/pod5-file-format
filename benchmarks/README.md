POD5 Benchmarks
==============

Building the benchmark environment
----------------------------------

To run benchmarks you first have to build the docker environment to run them:

```bash
> ./build.sh
```


Running a benchmark
-------------------

To run a benchmark, use the helper script to start the docker image:

```bash
> ./run_benchmark.sh convert ./path-to-source-files/
```


Benchmarking Results
--------------------

    Note preliminary results

    Results run on:
        0.0.16 POD5
        pyslow5 dev branch (commit 2643310a)

    Benchmark numbers are produced using a GridION.

    Note the benchmarks are run using python APIs, more work is required on C benchmarks.


## PCR Dataset

On dataset a PCR Zymo dataset PAM50264, on 10.4.1 e8.2 data (`pcr_zymo/20220419_1706_2E_PAM50264_3c6f33f1`):

### File sizes

| pod5   | blow5   | fast5   |
|--------|---------|---------|
| 37G    | 37G     | 52G     |

### Timings

|                                     | pod5       | blow5      | fast5      |
|-------------------------------------|------------|------------|------------|
| convert                             | 197.5 secs | 241.4 secs | Not Run    |
| find all read ids                   | 10.1 secs  | 1.8 secs   | 5.2 secs   |
| find all samples                    | 22.3 secs  | 82.5 secs  | 520.6 secs |
| find selected read ids read number  | 1.1 secs   | 5.8 secs   | 387.1 secs |
| find selected read ids sample count | 1.5 secs   | 5.7 secs   | 417.8 secs |
| find selected read ids samples      | 5.3 secs   | 6.4 secs   | 465.6 secs |

```* Note blow5 convert times include the index + merge operation```


## InterARTIC Dataset

Dataset available at:
https://github.com/Psy-Fer/interARTIC

### File sizes

| pod5   | blow5   | fast5   |
|--------|---------|---------|
| 3.3G   | 3.4G    | 6.9G    |

### Timings

|                                     | pod5      | blow5     | fast5     |
|-------------------------------------|-----------|-----------|-----------|
| convert                             | 28.6 secs | 21.0 secs | Not Run   |
| find all read ids                   | 0.5 secs  | 0.5 secs  | 0.7 secs  |
| find all samples                    | 3.0 secs  | 8.0 secs  | 73.5 secs |
| find selected read ids read number  | 0.4 secs  | 1.3 secs  | 29.3 secs |
| find selected read ids sample count | 0.6 secs  | 1.3 secs  | 30.4 secs |
| find selected read ids samples      | 1.4 secs  | 1.3 secs  | 37.8 secs |

```* Note blow5 convert times include the index + merge operation```
