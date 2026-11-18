"""Teste de integração da carga: precisa de um Postgres em WAREHOUSE_URL."""

import os

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from ingestion import sidra

WAREHOUSE_URL = os.environ.get("WAREHOUSE_URL")
pytestmark = pytest.mark.skipif(not WAREHOUSE_URL, reason="WAREHOUSE_URL não definido")


def test_load_e_idempotente_mesmo_com_view_dependente(monkeypatch):
    monkeypatch.setattr(sidra, "RAW_TABLE", "sidra_observacoes_teste")
    engine = create_engine(WAREHOUSE_URL)
    df = pd.DataFrame(
        {"indicador": ["taxa_desocupacao"] * 2, "uf_codigo": [35, 33], "valor": [6.2, 9.1]}
    )
    try:
        assert sidra.load(df, WAREHOUSE_URL) == 2
        # o dbt cria views sobre a tabela bruta; a segunda carga não pode falhar por causa delas
        with engine.begin() as conn:
            conn.execute(
                text("create view raw.v_teste as select * from raw.sidra_observacoes_teste")
            )
        assert sidra.load(df.head(1), WAREHOUSE_URL) == 1
        with engine.connect() as conn:
            assert conn.execute(text("select count(*) from raw.v_teste")).scalar() == 1
    finally:
        with engine.begin() as conn:
            conn.execute(text("drop view if exists raw.v_teste"))
            conn.execute(text("drop table if exists raw.sidra_observacoes_teste"))
