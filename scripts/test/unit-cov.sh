pytest \
    -vv \
    --cov=dagfactory \
    --cov-report=term-missing \
    --cov-report=xml \
    --ignore=tests/test_example_dags.py
