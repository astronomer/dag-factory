#!/bin/bash

set -v
set -x
set -e

if [ -L "dags" ]; then
    echo "Symbolic link 'dags' already exists."
elif [ -e "dags" ]; then
    echo "'dags' exists but is not a symbolic link. Please resolve this manually."
else
    ln -s dev/dags dags
    echo "Symbolic link 'dags' created successfully."
fi

rm -rf airflow.*
pip freeze | grep airflow
airflow db reset -y
# airflow db init
