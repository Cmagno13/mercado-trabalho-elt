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

# Uma cor por grande região, igual em todos os gráficos do dashboard.
CORES_REGIAO = {
    "Norte": "#2A9D8F",
    "Nordeste": "#E76F51",
    "Sudeste": "#264653",
    "Sul": "#E9A23B",
    "Centro-Oeste": "#7B5EA7",
}
COR_DESTAQUE = "#E76F51"

# Nomes de exibição das colunas do dataset (aparecem na tabela e nos filtros).
NOMES_COLUNAS = {
    "uf_sigla": "UF",
    "uf_nome": "Unidade da federação",
    "regiao": "Região",
    "trimestre_inicio": "Trimestre",
    "taxa_desocupacao": "Desocupação (%)",
    "taxa_desocupacao_var_anual_pp": "Var. anual (p.p.)",
    "rendimento_medio_real": "Rendimento (R$)",
    "rendimento_medio_real_var_anual_pct": "Var. anual (%)",
}

VERDE, VERMELHO = "#2A9D8F", "#E76F51"

DASHBOARD_CSS = """
/* Faixa de título */
.dashboard-header-container .header-with-actions {
  background: linear-gradient(90deg, #264653 0%, #2A9D8F 100%) !important;
}
.header-with-actions .title-panel input {
  background: transparent !important;
  color: #FFFFFF !important;
  -webkit-text-fill-color: #FFFFFF;
  /* o título aparece inteiro: o campo acompanha o tamanho do texto */
  field-sizing: content;
  width: auto !important;
  max-width: none !important;
  text-overflow: clip;
}
.header-with-actions .title-panel { flex-wrap: wrap; row-gap: 4px; }
.header-with-actions .title-panel .anticon,
.header-with-actions .title-panel [role="button"] svg { color: #FFFFFF; }

/* Cartões dos gráficos */
.dashboard-component-chart-holder {
  border-radius: 14px;
  border: 1px solid rgba(38, 70, 83, 0.12);
  box-shadow: 0 4px 14px rgba(38, 70, 83, 0.10);
  transition: box-shadow 0.2s ease, transform 0.2s ease;
}
.dashboard-component-chart-holder:hover {
  box-shadow: 0 10px 24px rgba(38, 70, 83, 0.18);
  transform: translateY(-2px);
}
.dashboard-component-chart-holder .header-title {
  font-weight: 600;
  letter-spacing: 0.01em;
}

/* KPIs: faixa colorida no topo, valor centralizado e fonte que acompanha a largura da tela */
.dashboard-component-chart-holder:has(.superset-legacy-chart-big-number) {
  border-top: 5px solid #2A9D8F;
}
.grid-row > :nth-child(1) .dashboard-component-chart-holder:has(.superset-legacy-chart-big-number),
.grid-row > :nth-child(3) .dashboard-component-chart-holder:has(.superset-legacy-chart-big-number) {
  border-top-color: #E76F51;
}
.dashboard-component-chart-holder:has(.superset-legacy-chart-big-number) .header-title {
  width: 100%;
  text-align: center;
}
.superset-legacy-chart-big-number,
.superset-legacy-chart-big-number .text-container {
  align-items: center !important;
  justify-content: center !important;
  text-align: center;
  width: 100%;
}
.superset-legacy-chart-big-number .header-line {
  justify-content: center;
  width: 100%;
  font-size: clamp(30px, 3.4vw, 58px) !important;
  font-weight: 700;
  color: #264653;
}
.superset-legacy-chart-big-number .subheader-line,
.superset-legacy-chart-big-number .subtitle-line {
  justify-content: center;
  width: 100%;
  font-size: clamp(11px, 0.95vw, 15px) !important;
  opacity: 0.7;
}
"""


def responsive_css(charts: list[tuple[int, dict]], breakpoint: int = 900) -> str:
    """CSS para telas estreitas: empilha os gráficos e amplia cada um até a largura da tela.

    O Superset já redimensiona os gráficos na proporção da janela. Abaixo do breakpoint
    as colunas ficariam estreitas demais, então cada linha quebra e o cartão é ampliado
    com `zoom` (os gráficos são desenhados em canvas com largura fixa em pixels, por isso
    mudar só a largura do cartão não basta). KPIs ficam dois por linha.
    """
    kpi_card = ".dashboard-component-chart-holder:has(.superset-legacy-chart-big-number)"
    rules = [
        ".grid-row { flex-wrap: wrap !important; row-gap: 16px; }",
        # com o zoom, o título e o valor dos KPIs precisam de fonte menor para caber no cartão
        f"{kpi_card} .header-title {{ font-size: 10px; line-height: 1.25; }}",
        ".superset-legacy-chart-big-number .header-line { font-size: 22px !important; }",
        ".superset-legacy-chart-big-number .subtitle-line { font-size: 8px !important; }",
    ]
    for chart_id, spec in charts:
        per_row = 2 if spec["viz_type"] == "big_number_total" else 1
        zoom = round(12 / (spec["width"] * per_row) * 0.96, 3)
        rules.append(
            f".resizable-container:has(.dashboard-chart-id-{chart_id}) {{ zoom: {zoom}; }}"
        )
    body = "\n  ".join(rules)
    return f"\n/* Telas estreitas */\n@media (max-width: {breakpoint}px) {{\n  {body}\n}}\n"


def metric(aggregate: str, column: str, label: str) -> dict:
    return {
        "expressionType": "SIMPLE",
        "aggregate": aggregate,
        "column": {"column_name": column},
        "label": label,
        "optionName": f"metric_{aggregate.lower()}_{column}",
    }


def kpi(name: str, aggregate: str, column: str, number_format: str) -> dict:
    return {
        "name": name,
        "viz_type": "big_number_total",
        "params": {
            "metric": metric(aggregate, column, name),
            "adhoc_filters": [ULTIMO_TRIMESTRE],
            "y_axis_format": number_format,
            "header_font_size": 0.5,
            "subtitle": "último trimestre disponível",
            "subtitle_font_size": 0.2,
        },
        "width": 3,
        "height": 20,
    }


def serie_por_regiao(name: str, column: str, label: str, number_format: str) -> dict:
    return {
        "name": name,
        "viz_type": "echarts_timeseries_smooth",
        "params": {
            "x_axis": "trimestre_inicio",
            "time_grain_sqla": "P3M",
            "metrics": [metric("AVG", column, label)],
            "groupby": ["regiao"],
            "adhoc_filters": [],
            "row_limit": 10000,
            "y_axis_format": number_format,
            "x_axis_time_format": "%Y",
            "show_legend": True,
            "legendOrientation": "top",
            "markerEnabled": False,
            "rich_tooltip": True,
            # a escala começa perto do menor valor: as diferenças entre regiões ficam visíveis
            "truncateYAxis": True,
            "y_axis_bounds": [None, None],
        },
        "width": 6,
        "height": 46,
    }


def chart_specs() -> list[dict]:
    """Gráficos nativos do Superset, na ordem em que entram no dashboard."""
    return [
        kpi("Maior taxa de desocupação (%)", "MAX", "taxa_desocupacao", ".1f"),
        kpi("Menor taxa de desocupação (%)", "MIN", "taxa_desocupacao", ".1f"),
        kpi("Menor rendimento médio (R$)", "MIN", "rendimento_medio_real", ",.0f"),
        kpi("Maior rendimento médio (R$)", "MAX", "rendimento_medio_real", ",.0f"),
        serie_por_regiao(
            "Taxa de desocupação por região (média simples das UFs)",
            "taxa_desocupacao",
            "Taxa média (%)",
            ".1f",
        ),
        serie_por_regiao(
            "Rendimento médio real por região (R$, média simples das UFs)",
            "rendimento_medio_real",
            "Rendimento médio (R$)",
            ",.0f",
        ),
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
                "show_value": True,
            },
            "width": 7,
            "height": 54,
        },
        {
            "name": "Retrato das UFs no último trimestre",
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
                "page_length": 0,
                "include_search": True,
                "show_cell_bars": False,
                "column_config": {
                    "taxa_desocupacao": {"d3NumberFormat": ".1f"},
                    "taxa_desocupacao_var_anual_pp": {"d3NumberFormat": "+.1f"},
                    "rendimento_medio_real": {"d3NumberFormat": ",.0f"},
                    "rendimento_medio_real_var_anual_pct": {"d3NumberFormat": "+.1f"},
                },
                # desocupação caindo e rendimento subindo em verde; o contrário em vermelho
                "conditional_formatting": [
                    {
                        "column": "taxa_desocupacao_var_anual_pp",
                        "operator": "<",
                        "targetValue": 0,
                        "colorScheme": VERDE,
                    },
                    {
                        "column": "taxa_desocupacao_var_anual_pp",
                        "operator": ">",
                        "targetValue": 0,
                        "colorScheme": VERMELHO,
                    },
                    {
                        "column": "rendimento_medio_real_var_anual_pct",
                        "operator": ">",
                        "targetValue": 0,
                        "colorScheme": VERDE,
                    },
                    {
                        "column": "rendimento_medio_real_var_anual_pct",
                        "operator": "<",
                        "targetValue": 0,
                        "colorScheme": VERMELHO,
                    },
                ],
            },
            "width": 5,
            "height": 54,
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

    def upsert(self, resource: str, column: str, value: str, payload: dict) -> int:
        """Cria o objeto ou atualiza o existente, para o script refletir mudanças no código."""
        existing = self.find(resource, column, value)
        if existing is None:
            return self.call("POST", f"{resource}/", json=payload)["id"]
        self.call("PUT", f"{resource}/{existing}", json=payload)
        return existing

    def label_columns(self, dataset_id: int) -> None:
        """Atualiza as colunas a partir do banco e aplica os nomes de exibição."""
        self.call("PUT", f"dataset/{dataset_id}/refresh")
        columns = self.call("GET", f"dataset/{dataset_id}")["result"]["columns"]
        payload = [
            {
                "id": c["id"],
                "column_name": c["column_name"],
                "verbose_name": NOMES_COLUNAS.get(c["column_name"], c.get("verbose_name")),
            }
            for c in columns
        ]
        self.call("PUT", f"dataset/{dataset_id}", json={"columns": payload})


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
    api.label_columns(dataset_id)

    charts = []
    for spec in chart_specs():
        params = {"datasource": f"{dataset_id}__table", "viz_type": spec["viz_type"]}
        params.update(spec["params"])
        chart_id = api.upsert(
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

    dashboard_id = api.upsert(
        "dashboard",
        "slug",
        DASHBOARD_SLUG,
        {
            "dashboard_title": DASHBOARD_TITLE,
            "slug": DASHBOARD_SLUG,
            "published": True,
            "position_json": json.dumps(layout(charts)),
            "css": DASHBOARD_CSS + responsive_css(charts),
            "json_metadata": json.dumps(
                {
                    "label_colors": {**CORES_REGIAO, "Taxa (%)": COR_DESTAQUE},
                    "cross_filters_enabled": True,
                }
            ),
        },
    )
    for chart_id, _ in charts:
        api.call("PUT", f"chart/{chart_id}", json={"dashboards": [dashboard_id]})
    # gráficos que saíram do código deixam o dashboard (não são apagados do Superset)
    atuais = {chart_id for chart_id, _ in charts}
    for chart in api.call("GET", f"dashboard/{dashboard_id}/charts")["result"]:
        if chart["id"] not in atuais:
            api.call("PUT", f"chart/{chart['id']}", json={"dashboards": []})

    print(f"Dashboard pronto: /superset/dashboard/{DASHBOARD_SLUG}/ ({len(charts)} gráficos)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
