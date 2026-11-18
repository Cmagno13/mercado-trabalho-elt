-- O Brasil tem 27 unidades da federação; menos que isso indica carga parcial.
select count(*) as ufs
from {{ ref('dim_uf') }}
having count(*) <> 27
