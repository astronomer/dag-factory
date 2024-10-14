pytest \
    -vv \
    --cov=dagfactory \
    --cov-report=term-missing \
    --cov-report=xml \
    --durations=0 \
    -m "not (integration or perf)" \
    --ignore=tests/test_example_dags.py
