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

On dataset a PCR Zymo dataset PAM50264, on 10.4.1 e8.2 data (`pcr_zymo/20220419_1706_2E_PAM50264_3c6f33f1`):

```
Note preliminary results

More work needed on splitting one blow5 file into batches for threading, and thoughts needed on if an index could be added to mkr files... or the benchmark could be better at least.
```

### Convert

|      | Fast5 | MKR      | blow5    |
|------|-------|----------|----------|
| Time | N/A   | 227 secs | 781 secs |
| Size | 52 GB | 37GB     | 38GB     |


### Find all read ids

|      | Fast5    | MKR      | blow5    |
|------|----------|----------|----------|
| Time | 6.1 secs | 4.9 secs | 275 secs |

### Find all samples

|      | Fast5    | MKR     | blow5    |
|------|----------|---------|----------|
| Time | 524 secs | 31 secs | 317 secs |


### Find selected read ids + extract read number

|      | Fast5    | MKR     | blow5  |
|------|----------|---------|--------|
| Time | 412 secs | 10 secs | 8 secs |

### Find selected read ids + extract sample count

|      | Fast5    | MKR     | blow5  |
|------|----------|---------|--------|
| Time | 414 secs | 14 secs | 9 secs |

### Find selected read ids + samples

|      | Fast5    | MKR     | blow5   |
|------|----------|---------|---------|
| Time | 476 secs | 16 secs | 10 secs |
