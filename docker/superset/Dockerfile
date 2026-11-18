FROM apache/superset:6.1.0

USER root
# A imagem oficial não traz driver de banco; o Postgres é usado nos metadados e no warehouse.
RUN uv pip install --python /app/.venv/bin/python psycopg2-binary requests
USER superset
