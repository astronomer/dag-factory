#!/usr/bin/env bash

airflow initdb
airflow scheduler & exec airflow webserver