#!/bin/bash

set -v
set -x
set -e

rm -rf airflow.*
pip freeze | grep airflow
airflow db reset -y
airflow db init
