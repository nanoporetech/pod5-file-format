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



Benchmarking Result
-------------------

### Converting fast5

fast5 dataset size: 11.0gb

fast5 -> mkr: 75s, 9.1gb
fast5 -> blow5: 80s, 8.2gb

### Finding read ids

mkr: 2.2s, 123699 ids
slow5: 80s, 123999 ids
