select distinct
    o.uf_codigo,
    o.uf_nome,
    r.uf_sigla,
    r.regiao
from {{ ref('stg_sidra__observacoes') }} as o
inner join {{ ref('uf_regiao') }} as r using (uf_codigo)
