pytest \
    -vv \
    --durations=0 \
    -m "not (integration or perf)" \
    --ignore=tests/test_example_dags.py
