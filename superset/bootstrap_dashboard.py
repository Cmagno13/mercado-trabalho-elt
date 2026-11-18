"""Cria no Superset, pela API REST, a conexão com o warehouse, o dataset, os gráficos e o dashboard.

Pode ser executado mais de uma vez: cada objeto é procurado pelo nome antes de ser criado.
Espera o mart existir, porque na primeira subida o Airflow ainda está rodando o pipeline.
"""

import json
import os
import sys
import time

import requests

BASE = os.environ["SUPERSET_URL"].rstrip("/")
USER = os.environ["SUPERSET_ADMIN_USER"]
PASSWORD = os.environ["SUPERSET_ADMIN_PASSWORD"]
WAREHOUSE_URI = os.environ["WAREHOUSE_SQLALCHEMY_URI"]

DATABASE_NAME = "Warehouse"
SCHEMA = "marts"
TABLE = "fct_mercado_trabalho_uf_trimestre"
DASHBOARD_TITLE = "Mercado de trabalho por UF — PNAD Contínua"
DASHBOARD_SLUG = "mercado-trabalho-uf"
WAIT_SECONDS = 15 * 60

ULTIMO_TRIMESTRE = {
    "expressionType": "SQL",
    "clause": "WHERE",
    "sqlExpression": "ultimo_trimestre = true",
}


def metric(aggregate: str, column: str, label: str) -> dict:
    return {
        "expressionType": "SIMPLE",
        "aggregate": aggregate,
        "column": {"column_name": column},
        "label": label,
        "optionName": f"metric_{aggregate.lower()}_{column}",
    }


def chart_specs() -> list[dict]:
    """Gráficos nativos do Superset: (nome, tipo, parâmetros, largura, altura)."""
    return [
        {
            "name": "Maior taxa de desocupação entre as UFs (%)",
            "viz_type": "big_number_total",
            "params": {
                "metric": metric("MAX", "taxa_desocupacao", "Maior taxa"),
                "adhoc_filters": [ULTIMO_TRIMESTRE],
                "y_axis_format": ".1f",
                "subheader": "Último trimestre disponível",
            },
            "width": 3,
            "height": 22,
        },
        {
            "name": "Menor taxa de desocupação entre as UFs (%)",
            "viz_type": "big_number_total",
            "params": {
                "metric": metric("MIN", "taxa_desocupacao", "Menor taxa"),
                "adhoc_filters": [ULTIMO_TRIMESTRE],
                "y_axis_format": ".1f",
                "subheader": "Último trimestre disponível",
            },
            "width": 3,
            "height": 22,
        },
        {
            "name": "Taxa de desocupação por região (média simples das UFs)",
            "viz_type": "echarts_timeseries_line",
            "params": {
                "x_axis": "trimestre_inicio",
                "time_grain_sqla": "P3M",
                "metrics": [metric("AVG", "taxa_desocupacao", "Taxa média (%)")],
                "groupby": ["regiao"],
                "adhoc_filters": [],
                "row_limit": 10000,
                "y_axis_format": ".1f",
                "show_legend": True,
            },
            "width": 6,
            "height": 44,
        },
        {
            "name": "Taxa de desocupação por UF no último trimestre (%)",
            "viz_type": "echarts_timeseries_bar",
            "params": {
                "x_axis": "uf_sigla",
                "metrics": [metric("MAX", "taxa_desocupacao", "Taxa (%)")],
                "groupby": [],
                "adhoc_filters": [ULTIMO_TRIMESTRE],
                "row_limit": 100,
                "x_axis_sort": "Taxa (%)",
                "x_axis_sort_asc": False,
                "y_axis_format": ".1f",
                "show_legend": False,
            },
            "width": 12,
            "height": 44,
        },
        {
            "name": "Rendimento médio real por região (R$, média simples das UFs)",
            "viz_type": "echarts_timeseries_line",
            "params": {
                "x_axis": "trimestre_inicio",
                "time_grain_sqla": "P3M",
                "metrics": [metric("AVG", "rendimento_medio_real", "Rendimento médio (R$)")],
                "groupby": ["regiao"],
                "adhoc_filters": [],
                "row_limit": 10000,
                "y_axis_format": ",.0f",
                "show_legend": True,
            },
            "width": 6,
            "height": 50,
        },
        {
            "name": "UFs no último trimestre",
            "viz_type": "table",
            "params": {
                "query_mode": "raw",
                "all_columns": [
                    "uf_sigla",
                    "regiao",
                    "taxa_desocupacao",
                    "taxa_desocupacao_var_anual_pp",
                    "rendimento_medio_real",
                    "rendimento_medio_real_var_anual_pct",
                ],
                "adhoc_filters": [ULTIMO_TRIMESTRE],
                "order_by_cols": [json.dumps(["taxa_desocupacao", False])],
                "row_limit": 100,
                "table_timestamp_format": "smart_date",
            },
            "width": 6,
            "height": 50,
        },
    ]


class Superset:
    def __init__(self) -> None:
        self.http = requests.Session()
        token = self.http.post(
            f"{BASE}/api/v1/security/login",
            json={"username": USER, "password": PASSWORD, "provider": "db", "refresh": True},
            timeout=30,
        )
        token.raise_for_status()
        self.http.headers["Authorization"] = f"Bearer {token.json()['access_token']}"
        csrf = self.http.get(f"{BASE}/api/v1/security/csrf_token/", timeout=30)
        csrf.raise_for_status()
        self.http.headers["X-CSRFToken"] = csrf.json()["result"]
        self.http.headers["Referer"] = BASE

    def call(self, method: str, path: str, **kwargs) -> dict:
        response = self.http.request(method, f"{BASE}/api/v1/{path}", timeout=60, **kwargs)
        if not response.ok:
            raise RuntimeError(f"{method} {path} -> {response.status_code}: {response.text[:500]}")
        return response.json()

    def find(self, resource: str, column: str, value: str) -> int | None:
        query = json.dumps({"filters": [{"col": column, "opr": "eq", "value": value}]})
        result = self.call("GET", f"{resource}/", params={"q": query})["result"]
        return result[0]["id"] if result else None

    def get_or_create(self, resource: str, column: str, value: str, payload: dict) -> int:
        existing = self.find(resource, column, value)
        if existing is not None:
            return existing
        return self.call("POST", f"{resource}/", json=payload)["id"]


def wait_for_dataset(api: Superset, database_id: int) -> int:
    """Registra o dataset; tenta de novo enquanto o dbt ainda não criou a tabela."""
    payload = {"database": database_id, "schema": SCHEMA, "table_name": TABLE}
    deadline = time.monotonic() + WAIT_SECONDS
    while True:
        try:
            return api.get_or_create("dataset", "table_name", TABLE, payload)
        except RuntimeError as error:
            if time.monotonic() > deadline:
                raise
            print(f"Aguardando {SCHEMA}.{TABLE} existir no warehouse ({error})", flush=True)
            time.sleep(15)


def layout(charts: list[tuple[int, dict]]) -> dict:
    """Monta o position_json: linhas de até 12 colunas, na ordem dos gráficos."""
    position = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": [], "parents": ["ROOT_ID"]},
        "HEADER_ID": {"type": "HEADER", "id": "HEADER_ID", "meta": {"text": DASHBOARD_TITLE}},
    }
    row_id, used = None, 12
    for chart_id, spec in charts:
        if used + spec["width"] > 12:
            row_id = f"ROW-{len(position['GRID_ID']['children']) + 1}"
            position["GRID_ID"]["children"].append(row_id)
            position[row_id] = {
                "type": "ROW",
                "id": row_id,
                "children": [],
                "parents": ["ROOT_ID", "GRID_ID"],
                "meta": {"background": "BACKGROUND_TRANSPARENT"},
            }
            used = 0
        key = f"CHART-{chart_id}"
        position[row_id]["children"].append(key)
        position[key] = {
            "type": "CHART",
            "id": key,
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID", row_id],
            "meta": {
                "chartId": chart_id,
                "width": spec["width"],
                "height": spec["height"],
                "sliceName": spec["name"],
            },
        }
        used += spec["width"]
    return position


def main() -> int:
    api = Superset()
    database_id = api.get_or_create(
        "database",
        "database_name",
        DATABASE_NAME,
        {"database_name": DATABASE_NAME, "sqlalchemy_uri": WAREHOUSE_URI, "expose_in_sqllab": True},
    )
    dataset_id = wait_for_dataset(api, database_id)

    charts = []
    for spec in chart_specs():
        params = {"datasource": f"{dataset_id}__table", "viz_type": spec["viz_type"]}
        params.update(spec["params"])
        chart_id = api.get_or_create(
            "chart",
            "slice_name",
            spec["name"],
            {
                "slice_name": spec["name"],
                "viz_type": spec["viz_type"],
                "datasource_id": dataset_id,
                "datasource_type": "table",
                "params": json.dumps(params),
            },
        )
        charts.append((chart_id, spec))

    dashboard_id = api.get_or_create(
        "dashboard",
        "slug",
        DASHBOARD_SLUG,
        {
            "dashboard_title": DASHBOARD_TITLE,
            "slug": DASHBOARD_SLUG,
            "published": True,
            "position_json": json.dumps(layout(charts)),
        },
    )
    for chart_id, _ in charts:
        api.call("PUT", f"chart/{chart_id}", json={"dashboards": [dashboard_id]})

    print(f"Dashboard pronto: /superset/dashboard/{DASHBOARD_SLUG}/ ({len(charts)} gráficos)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
