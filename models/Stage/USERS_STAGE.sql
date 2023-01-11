with source as 

(
select *
FROM {{source('raw', 'USERS') }}
) 
, 

users_stage as
(
select
user::varchar as USER, 
gender::varchar as GENDER, 
age::varchar as AGE, 
zip::varchar as ZIP, 
company::varchar as COMPANY
from source
)

select * 
FROM users_stage