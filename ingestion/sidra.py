"""Ingestão das séries da PNAD Contínua (IBGE/SIDRA) para o schema `raw` do warehouse.

Carga completa a cada execução: as séries são pequenas (27 UFs x trimestres desde 2012)
e o IBGE revisa valores passados, então substituir o conteúdo é mais simples e mais correto
do que tentar uma carga incremental.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime

import pandas as pd
import requests
from sqlalchemy import create_engine, inspect, text

SIDRA_URL = "https://apisidra.ibge.gov.br/values"

# indicador -> caminho da consulta na API do SIDRA
SERIES = {
    # Tabela 4099: taxa de desocupação (%), pessoas de 14 anos ou mais
    "taxa_desocupacao": "t/4099/n3/all/v/4099/p/all",
    # Tabela 5436: rendimento médio mensal real habitual (R$), sexo = total
    "rendimento_medio_real": "t/5436/n3/all/v/5933/p/all/c2/6794",
}

RAW_SCHEMA = "raw"
RAW_TABLE = "sidra_observacoes"


def fetch(path: str, timeout: int = 60) -> list[dict]:
    """Baixa uma consulta do SIDRA. O primeiro registro da resposta é o cabeçalho."""
    response = requests.get(f"{SIDRA_URL}/{path}", timeout=timeout)
    response.raise_for_status()
    return response.json()[1:]


def to_frame(records: list[dict], indicador: str) -> pd.DataFrame:
    """Converte os registros do SIDRA em um DataFrame tabular e tipado.

    O SIDRA devolve tudo como texto e usa marcadores como "..." e "-" para valor
    indisponível; esses viram NaN em vez de derrubar a carga.
    """
    df = pd.DataFrame.from_records(records)
    out = pd.DataFrame(
        {
            "indicador": indicador,
            "uf_codigo": df["D1C"].astype(int),
            "uf_nome": df["D1N"].str.strip(),
            "trimestre_codigo": df["D3C"].astype(str),
            "unidade": df["MN"].str.strip(),
            "valor": pd.to_numeric(df["V"], errors="coerce"),
        }
    )
    return out


def extract() -> pd.DataFrame:
    frames = [to_frame(fetch(path), indicador) for indicador, path in SERIES.items()]
    df = pd.concat(frames, ignore_index=True)
    df["carregado_em"] = datetime.now(UTC)
    return df


def load(df: pd.DataFrame, warehouse_url: str) -> int:
    """Substitui o conteúdo da tabela bruta em uma única transação.

    Usa TRUNCATE + INSERT em vez de recriar a tabela: as views de staging do dbt
    dependem dela, e o Postgres recusa o DROP de uma tabela com dependentes.
    """
    engine = create_engine(warehouse_url)
    with engine.begin() as conn:
        conn.execute(text(f"create schema if not exists {RAW_SCHEMA}"))
        if inspect(conn).has_table(RAW_TABLE, schema=RAW_SCHEMA):
            conn.execute(text(f"truncate table {RAW_SCHEMA}.{RAW_TABLE}"))
        df.to_sql(RAW_TABLE, conn, schema=RAW_SCHEMA, if_exists="append", index=False)
    return len(df)


def run() -> int:
    df = extract()
    if df.empty:
        raise RuntimeError("O SIDRA não devolveu nenhum registro; carga abortada.")
    rows = load(df, os.environ["WAREHOUSE_URL"])
    print(f"{rows} linhas carregadas em {RAW_SCHEMA}.{RAW_TABLE}")
    return rows


if __name__ == "__main__":
    run()
