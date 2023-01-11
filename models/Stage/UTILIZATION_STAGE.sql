with source as 

(
select *
FROM {{source('raw', 'UTILIZATION') }}
) 
, 

util_stage as
(
select 
user::varchar as USER, 
event_date::varchar as EVENT_DATE, 
event::varchar as EVENT
from source 
)

select * 
FROM util_stage