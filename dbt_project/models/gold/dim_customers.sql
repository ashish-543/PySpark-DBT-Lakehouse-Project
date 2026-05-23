SELECT
    ROW_NUMBER() OVER (ORDER BY crm_cst.customer_id) AS sk_customer_key,
    crm_cst.customer_id,
    crm_cst.customer_number,
    crm_cst.first_name,
    crm_cst.last_name,
    erp_loc.country,
    crm_cst.marital_status,
    CASE
        WHEN crm_cst.gender <> 'n/a' THEN crm_cst.gender
        ELSE COALESCE(erp_cst.gender, 'n/a')
    END AS gender,
    erp_cst.birth_date AS birthdate,
    crm_cst.create_date
FROM {{source("silver", "crm_customers")}} AS crm_cst
LEFT JOIN {{source("silver", "erp_customers")}} AS erp_cst
    ON crm_cst.customer_number = erp_cst.customer_number
LEFT JOIN {{source("silver", "erp_customer_location")}} AS erp_loc
    ON crm_cst.customer_number = erp_loc.customer_number