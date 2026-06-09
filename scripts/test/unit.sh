pytest \
    -vv \
    -m "not integration" \
    --ignore=tests/test_example_dags.py
