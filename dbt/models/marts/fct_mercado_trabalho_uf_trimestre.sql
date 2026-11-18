with observacoes as (

    select * from {{ ref('stg_sidra__observacoes') }}

),

pivotado as (

    select
        uf_codigo,
        trimestre_codigo,
        ano,
        trimestre,
        trimestre_inicio,
        max(valor) filter (where indicador = 'taxa_desocupacao')      as taxa_desocupacao,
        max(valor) filter (where indicador = 'rendimento_medio_real') as rendimento_medio_real
    from observacoes
    group by 1, 2, 3, 4, 5

),

com_variacao as (

    select
        *,
        -- mesmo trimestre do ano anterior: a taxa de desocupação é sazonal
        lag(taxa_desocupacao, 4) over (
            partition by uf_codigo order by trimestre_inicio
        ) as taxa_desocupacao_ano_anterior,
        lag(rendimento_medio_real, 4) over (
            partition by uf_codigo order by trimestre_inicio
        ) as rendimento_medio_real_ano_anterior
    from pivotado

)

select
    v.uf_codigo || '-' || v.trimestre_codigo as uf_trimestre_id,
    v.uf_codigo,
    d.uf_sigla,
    d.uf_nome,
    d.regiao,
    v.trimestre_codigo,
    v.ano,
    v.trimestre,
    v.trimestre_inicio,
    -- facilita filtrar o retrato mais recente nos painéis
    v.trimestre_inicio = max(v.trimestre_inicio) over () as ultimo_trimestre,
    v.taxa_desocupacao,
    v.rendimento_medio_real,
    v.taxa_desocupacao - v.taxa_desocupacao_ano_anterior as taxa_desocupacao_var_anual_pp,
    round(
        100.0 * (v.rendimento_medio_real - v.rendimento_medio_real_ano_anterior)
        / nullif(v.rendimento_medio_real_ano_anterior, 0),
        2
    ) as rendimento_medio_real_var_anual_pct
from com_variacao as v
inner join {{ ref('dim_uf') }} as d using (uf_codigo)
