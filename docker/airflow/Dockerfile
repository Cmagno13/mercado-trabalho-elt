FROM apache/airflow:slim-3.4.0-python3.12

ARG AIRFLOW_VERSION=3.4.0
ARG CONSTRAINTS=https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.12.txt

# Dependências do DAG (ingestão em pandas) no ambiente do Airflow, respeitando as constraints.
RUN pip install --no-cache-dir --constraint "${CONSTRAINTS}" \
    apache-airflow-providers-standard apache-airflow-providers-postgres asyncpg \
    pandas requests sqlalchemy psycopg2-binary

# O dbt fica em um virtualenv separado: as dependências dele conflitam com as do Airflow.
RUN python -m venv /home/airflow/dbt-venv \
    && /home/airflow/dbt-venv/bin/pip install --no-cache-dir "dbt-postgres>=1.9,<2"

# Destino dos artefatos do dbt (manifest e catalog), compartilhado por um volume.
RUN mkdir -p /opt/airflow/dbt-artifacts
