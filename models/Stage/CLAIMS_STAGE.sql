with source as 

(
select *
FROM {{source('raw', 'CLAIM') }}
) 
, 

claim_stage as
(
select
user::varchar as USER, 
CLAIM_NUMBER::varchar as CLAIM_NUMBER,
DIAGNOSIS_CODE_1::varchar as DIAGNOSIS_CODE_1,
IFNULL(DIAGNOSIS_CODE_2::varchar,'None') as DIAGNOSIS_CODE_2 ,
IFNULL(DIAGNOSIS_CODE_3::varchar,'None') as DIAGNOSIS_CODE_3,
IFNULL(PROCEDURE_CODE::varchar,'None') as PROCEDURE_CODE,
ALLOWED_AMOUNT::varchar as ALLOWED_AMOUNT , 
AMOUNT_PAID::varchar as AMOUNT_PAID,
SERVICE_BEGIN_DATE::varchar as SERVICE_BEGIN_DATE,
SERVICE_END_DATE::varchar as SERVICE_END_DATE,
DIAGNOSIS_DESCRIPTION::varchar as DIAGNOSIS_DESCRIPTION
from source 
)

select * 
FROM claim_stage