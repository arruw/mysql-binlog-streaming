#!/bin/sh

airflow checkdb

airflow initdb

nohup airflow scheduler &

airflow webserver