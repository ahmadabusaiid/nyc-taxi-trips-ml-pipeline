FROM apache/airflow:latest

WORKDIR /opt/airflow

RUN pip install duckdb
