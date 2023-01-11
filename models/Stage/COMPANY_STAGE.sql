with source as 

(
select *
FROM {{source('raw', 'COMPANY') }}
) 
, 

Company_stage as
(
select 
company::varchar as COMPANY , 
industry::varchar as INDUSTRY, 
size::varchar as SIZE
from source 
)

select * 
FROM company_stage