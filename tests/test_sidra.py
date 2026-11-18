import pandas as pd

from ingestion import sidra

RECORDS = [
    {"D1C": "35", "D1N": "São Paulo ", "D3C": "202403", "MN": "%", "V": "6.2"},
    {"D1C": "11", "D1N": "Rondônia", "D3C": "202403", "MN": "%", "V": "..."},
    {"D1C": "53", "D1N": "Distrito Federal", "D3C": "201201", "MN": "%", "V": "-"},
]


def test_to_frame_tipa_colunas():
    df = sidra.to_frame(RECORDS, "taxa_desocupacao")
    assert list(df.columns) == [
        "indicador",
        "uf_codigo",
        "uf_nome",
        "trimestre_codigo",
        "unidade",
        "valor",
    ]
    assert df["uf_codigo"].tolist() == [35, 11, 53]
    assert df["uf_nome"].iloc[0] == "São Paulo"
    assert df["trimestre_codigo"].iloc[0] == "202403"
    assert (df["indicador"] == "taxa_desocupacao").all()


def test_to_frame_marcadores_de_ausencia_viram_nan():
    df = sidra.to_frame(RECORDS, "taxa_desocupacao")
    assert df["valor"].iloc[0] == 6.2
    assert df["valor"].iloc[1:].isna().all()


def test_extract_concatena_as_series(monkeypatch):
    monkeypatch.setattr(sidra, "fetch", lambda path: RECORDS)
    df = sidra.extract()
    assert len(df) == len(RECORDS) * len(sidra.SERIES)
    assert set(df["indicador"]) == set(sidra.SERIES)
    assert pd.api.types.is_datetime64_any_dtype(df["carregado_em"])
