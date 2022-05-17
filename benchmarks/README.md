MKR Benchmarks
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



Benchmarking Results
--------------------

    Note preliminary results

    Results run on:
        0.0.13 MKR
        pyslow5 dev branch (commit 2643310a)

    Benchmark numbers are produced using a GridION.

    Note the benchmarks are run using python APIs, more work is required on C benchmarks.


## PCR Dataset

On dataset a PCR Zymo dataset PAM50264, on 10.4.1 e8.2 data (`pcr_zymo/20220419_1706_2E_PAM50264_3c6f33f1`):



### Convert

|      | Fast5 | MKR      | blow5     |
|------|-------|----------|-----------|
| Time | N/A   | 212 secs | 242 secs* |
| Size | 52 GB | 37GB     | 38GB      |

```* Note blow5 times include the index operation```


### Find all read ids

|      | Fast5    | MKR      | blow5    |
|------|----------|----------|----------|
| Time | 6.1 secs | 4.7 secs | 1.8 secs |

### Find all samples

|      | Fast5    | MKR     | blow5    |
|------|----------|---------|----------|
| Time | 524 secs | 26 secs | 78 secs |

### Find selected read ids + extract read number

|      | Fast5    | MKR      | blow5  |
|------|----------|----------|--------|
| Time | 412 secs | 2 secs   | 5 secs |

### Find selected read ids + extract sample count

|      | Fast5    | MKR       | blow5    |
|------|----------|-----------|----------|
| Time | 414 secs | 8.8 secs  | 5.8 secs |

### Find selected read ids + samples

|      | Fast5    | MKR      | blow5    |
|------|----------|----------|----------|
| Time | 476 secs | 6.2 secs | 6.1 secs |


## InterARTIC Dataset

Dataset available at:
https://github.com/Psy-Fer/interARTIC


### Convert

|      | Fast5 | MKR      | blow5    |
|------|-------|----------|----------|
| Time | N/A   | 24 secs  | 21 secs* |
| Size | 7 GB  | 3.3 GB   | 3.4 GB   |

```* Note blow5 times include the index operation```

### Find all read ids

|      | Fast5    | MKR       | blow5    |
|------|----------|-----------|----------|
| Time | 1 secs   | ~1 secs   | ~1 secs  |

### Find all samples

|      | Fast5    | MKR     | blow5    |
|------|----------|---------|----------|
| Time | 71 secs  | 3 secs  | 8 secs   |

### Find selected read ids + extract read number

|      | Fast5    | MKR     | blow5  |
|------|----------|---------|--------|
| Time | 29 secs  | 1 secs  | 1 secs |

### Find selected read ids + extract sample count

|      | Fast5    | MKR     | blow5  |
|------|----------|---------|--------|
| Time | 32 secs  | 1 secs  | 1 secs |

### Find selected read ids + samples

|      | Fast5    | MKR     | blow5   |
|------|----------|---------|---------|
| Time | 38 secs  | 1 secs  | 1 secs  |
