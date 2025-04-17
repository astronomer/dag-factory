#!/bin/bash

set -x

set -e

airflow dags list-import-errors
