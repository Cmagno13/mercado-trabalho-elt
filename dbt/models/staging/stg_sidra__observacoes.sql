with source as (

    select * from {{ source('raw', 'sidra_observacoes') }}

)

select
    indicador,
    uf_codigo::int                                  as uf_codigo,
    uf_nome,
    trimestre_codigo,
    left(trimestre_codigo, 4)::int                  as ano,
    right(trimestre_codigo, 2)::int                 as trimestre,
    -- primeiro dia do trimestre, para os gráficos de série temporal
    make_date(
        left(trimestre_codigo, 4)::int,
        (right(trimestre_codigo, 2)::int - 1) * 3 + 1,
        1
    )                                               as trimestre_inicio,
    valor::numeric                                  as valor
from source
where valor is not null
