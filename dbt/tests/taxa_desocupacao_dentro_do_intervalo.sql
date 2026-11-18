-- A taxa de desocupação é um percentual: qualquer valor fora de 0–100 indica erro de carga.
select uf_trimestre_id, taxa_desocupacao
from {{ ref('fct_mercado_trabalho_uf_trimestre') }}
where taxa_desocupacao < 0 or taxa_desocupacao > 100
