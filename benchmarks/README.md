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

    *More work needed on splitting one blow5 file into batches for threading*

    Results run on:
        0.0.13 MKR
        0.5.0a1 pyslow5


## PCR Dataset

On dataset a PCR Zymo dataset PAM50264, on 10.4.1 e8.2 data (`pcr_zymo/20220419_1706_2E_PAM50264_3c6f33f1`):



### Convert

|      | Fast5 | MKR      | blow5     |
|------|-------|----------|-----------|
| Time | N/A   | 212 secs | 772 secs* |
| Size | 52 GB | 37GB     | 38GB      |

```* Note blow5 times include merge and index operation, individual operation times: Convert 280s, Merge 275s, Index 217s```


### Find all read ids

|      | Fast5    | MKR      | blow5    |
|------|----------|----------|----------|
| Time | 6.1 secs | 4.7 secs | 275 secs |

### Find all samples

|      | Fast5    | MKR     | blow5    |
|------|----------|---------|----------|
| Time | 524 secs | 26 secs | 317 secs |

### Find selected read ids + extract read number

|      | Fast5    | MKR      | blow5  |
|------|----------|----------|--------|
| Time | 412 secs | 2.1 secs | 8 secs |

### Find selected read ids + extract sample count

|      | Fast5    | MKR       | blow5    |
|------|----------|-----------|----------|
| Time | 414 secs | 8.8 secs  | 8.7 secs |

### Find selected read ids + samples

|      | Fast5    | MKR      | blow5    |
|------|----------|----------|----------|
| Time | 476 secs | 8.2 secs | 9.6 secs |


## InterARTIC Dataset

Dataset available at:
https://github.com/Psy-Fer/interARTIC


### Convert

|      | Fast5 | MKR      | blow5    |
|------|-------|----------|----------|
| Time | N/A   | 26 secs  | 63 secs  |
| Size | 7 GB  | 3.3 GB   | 3.4 GB   |

```* Note blow5 times include merge and index operation, individual operation times: Convert 24s, Merge 20s, Index 18s```


### Find all read ids

|      | Fast5    | MKR      | blow5    |
|------|----------|----------|----------|
| Time | x   secs | x   secs | x   secs |

### Find all samples

|      | Fast5    | MKR     | blow5    |
|------|----------|---------|----------|
| Time | x   secs | x  secs | x   secs |


### Find selected read ids + extract read number

|      | Fast5    | MKR     | blow5  |
|------|----------|---------|--------|
| Time | x   secs | x  secs | x secs |

### Find selected read ids + extract sample count

|      | Fast5    | MKR     | blow5  |
|------|----------|---------|--------|
| Time | x   secs | x secs  | x secs |

### Find selected read ids + samples

|      | Fast5    | MKR     | blow5   |
|------|----------|---------|---------|
| Time | x   secs | x secs  | x  secs |
