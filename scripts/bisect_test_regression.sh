

git checkout main
git bisect bad
git checkout v0.9.2
git bisect good
git bisect run " (cd duckdb-main && GEN=ninja make benchmark && cd ..) && python duckdb-main/scripts/regression_test_runner.py \
                                                               --old=duckdb-old/build/release/benchmark/benchmark_runner \
                                                               --new=duckdb-main/build/release/benchmark/benchmark_runner \
                                                               --benchmarks=duckdb-main/.github/regression/large.csv"