select *
from {{source("silver", "crm_customers")}}
limit 10