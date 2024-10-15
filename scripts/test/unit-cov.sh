pytest \
    -vv \
    --cov=dagfactory \
    --cov-report=term-missing \
    --cov-report=xml \
    --durations=0 \
    --ignore=tests/test_example_dags.py
