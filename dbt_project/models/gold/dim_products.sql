SELECT
    ROW_NUMBER() OVER (ORDER BY product_id) AS sk_product_key, -- Surrogate key
    crm_prd.product_id,
    crm_prd.product_number,
    crm_prd.product_name,
    crm_prd.category_id,
    erp_prd.category,
    erp_prd.subcategory,
    erp_prd.maintenance_flag,
    crm_prd.product_line,
    crm_prd.start_date
FROM {{source("silver", "crm_products")}} AS crm_prd
LEFT JOIN {{source("silver", "erp_product_category")}} AS erp_prd
    ON crm_prd.category_id = erp_prd.category_id
WHERE crm_prd.end_date IS NULL