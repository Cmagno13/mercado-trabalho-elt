"""ELT do mercado de trabalho: ingestão em pandas (SIDRA/IBGE) e transformação em dbt."""

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task

DBT = "cd /opt/project/dbt && /home/airflow/dbt-venv/bin/dbt"
DBT_FLAGS = "--profiles-dir . --target-path /tmp/dbt-target --log-path /tmp/dbt-logs"

with DAG(
    dag_id="mercado_trabalho_elt",
    description="PNAD Contínua por UF: SIDRA -> raw (pandas) -> staging e marts (dbt)",
    # A PNAD Contínua trimestral sai uma vez por trimestre; a checagem semanal
    # é barata e pega revisões do IBGE.
    schedule="@weekly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["elt", "ibge", "dbt"],
) as dag:

    @task
    def ingerir_sidra() -> int:
        from ingestion import sidra

        return sidra.run()

    dbt_seed = BashOperator(task_id="dbt_seed", bash_command=f"{DBT} seed {DBT_FLAGS}")
    dbt_run = BashOperator(task_id="dbt_run", bash_command=f"{DBT} run {DBT_FLAGS}")
    dbt_test = BashOperator(task_id="dbt_test", bash_command=f"{DBT} test {DBT_FLAGS}")

    # manifest.json e catalog.json: documentação do dbt e entrada do catálogo (Amundsen)
    dbt_docs = BashOperator(
        task_id="dbt_docs",
        bash_command=(
            f"{DBT} docs generate --profiles-dir . --log-path /tmp/dbt-logs "
            "--target-path /opt/airflow/dbt-artifacts"
        ),
    )

    ingerir_sidra() >> dbt_seed >> dbt_run >> dbt_test >> dbt_docs
