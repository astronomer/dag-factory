pytest \
    -vv \
    --durations=0 \
    -m "not (integration or perf)" \
