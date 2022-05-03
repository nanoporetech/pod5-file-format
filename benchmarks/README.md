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

On dataset a PCR Zymo dataset PAM50264, on 10.4.1 e8.2 data (`pcr_zymo/20220419_1706_2E_PAM50264_3c6f33f1`):

### Converting fast5