#!/usr/bin/env bash

airflow db init
airflow scheduler & exec airflow webserver