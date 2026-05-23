SELECT
    crm_sales.order_number,
    dim_prd.sk_product_key,
    dim_cst.sk_customer_key,
    crm_sales.order_date,
    crm_sales.ship_date,
    crm_sales.due_date,
    crm_sales.sales_amount,
    crm_sales.quantity,
    crm_sales.price
FROM {{source("silver", "crm_sales")}} AS crm_sales
LEFT JOIN {{ref("dim_products")}} AS dim_prd
    ON crm_sales.product_number = dim_prd.product_number
LEFT JOIN {{ref("dim_customers")}} AS dim_cst
    ON crm_sales.customer_id = dim_cst.customer_id