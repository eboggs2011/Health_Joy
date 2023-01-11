/* 
1. What have I done to get the data into snowflake?
2. Create raw tables for the csv's 
3. Load them into raw tables 
4. Created a file format in snowflake to load the csv's individually
5. You could put the csv's in cloud (s3 bucket  or azure blob container) to automate but will not
6. Once tables are created and data is loaded in snowflake - I can reference these in DBT for staging/raw basic transformation 
7. Staging is for basic transformation (ie convert data types, handle nulls, standardize) 
8. Once Staged we can start with the SQL 
9. What is considered a service? Is a service an event or diagnosis_description, is it the procedure code? I'm assuming procedure?
10.What is considered most engaged? Who utilized the web site as in took the most events?

*/

-- First cte - joins user and company just to get the attributes from the company table -- Joined claim to get attributes and do some aggregations to get service counts as well as sums
-- Will have more than one user in this result set


with counts as 

(
select u.user as USER, 
u.company as COMPANY,
u.gender AS GENDER,
u.age AS AGE,
u.zip AS ZIPCODE,
company.size AS COMPANY_SIZE,
company.INDUSTRY AS COMPANY_INDUSTRY,
c.DIAGNOSIS_DESCRIPTION AS DIAGNOSIS_DESCRIPTION,
c.PROCEDURE_CODE AS PROCEDURE_CODE,
c.SERVICE_BEGIN_DATE,
c.SERVICE_END_DATE,
count(PROCEDURE_CODE) as SERVICE_COUNT,
SUM(ALLOWED_AMOUNT) as Total_Allowed_Amount,
SUM(Amount_Paid) as Total_Amount_Payed
from {{ref('USERS_STAGE')}} as u 
LEFT JOIN {{ref('COMPANY_STAGE')}} as company 
  on u.company = company.company
LEFT JOIN {{ref('CLAIMS_STAGE')}} as c 
on u.user = c.user 
--where u.user ='9f5ca1e6'
group by u.user,u.company,company.size,company.INDUSTRY,u.gender,u.age,u.zip,c.DIAGNOSIS_DESCRIPTION, c.PROCEDURE_CODE,c.SERVICE_BEGIN_DATE,c.SERVICE_END_DATE
  
  
  ),
  
  Total_Events as 

(
select 
user, 
SUM(Utilization_Count) as Total_Utilization_Events
from 
(
select 
u.user,
COUNT(*) as Utilization_Count
FROM {{ref('USERS_STAGE')}} as users
LEFT JOIN {{ref('UTILIZATION_STAGE')}} as u
ON users.user = u.user
--where u.user ='1bcf32da'
group by u.user
)
group by user 
)
  
 select 
c.user, 
C.company as COMPANY,
C.gender AS GENDER,
C.age AS AGE,
ZIPCODE AS ZIPCODE,
COMPANY_SIZE AS COMPANY_SIZE,
COMPANY_INDUSTRY AS COMPANY_INDUSTRY,
c.DIAGNOSIS_DESCRIPTION AS DIAGNOSIS_DESCRIPTION,
c.PROCEDURE_CODE AS PROCEDURE_CODE,
c.SERVICE_BEGIN_DATE,
c.SERVICE_END_DATE,
c.service_count,
te.TOTAL_UTILIZATION_EVENTS,
TOTAL_ALLOWED_AMOUNT,
TOTAL_AMOUNT_PAYED,
CURRENT_TIMESTAMP() as LAST_INSERTED
 FROM counts as c 
 LEFT JOIN total_events as te
 on c.user = te.user 
-- WHERE C.USER = '9f5ca1e6'