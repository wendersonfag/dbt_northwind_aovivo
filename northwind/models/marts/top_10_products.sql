WITH products as (
    SELECT * FROM {{ref("stg_crm__products")}}
),
order_details AS (
    SELECT * FROM {{ref("stg_crm_order_details")}}
)
SELECT 
    products.product_name
    ,SUM(order_details.unit_price * order_details.quantity * (1.0 - order_details.discount)) AS sales
FROM 
    products
INNER JOIN order_details 
ON order_details.product_id = products.product_id
GROUP BY 
    products.product_name
ORDER BY 
    sales DESC