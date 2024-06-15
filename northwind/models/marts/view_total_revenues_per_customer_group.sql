WITH customers AS (
    SELECT * FROM {{ref("stg_crm__customer")}}
),
order_details AS (
    SELECT * FROM {{ref("stg_crm_order_details")}}
),
orders AS (
    SELECT * FROM {{ref("stg_crm_orders")}}
)
SELECT 
customers.company_name, 
SUM(order_details.unit_price * order_details.quantity * (1.0 - order_details.discount)) AS total,
NTILE(5) OVER (ORDER BY SUM(order_details.unit_price * order_details.quantity * (1.0 - order_details.discount)) DESC) AS group_number
FROM 
    customers
INNER JOIN orders 
    ON customers.customer_id = orders.customer_id
INNER JOIN order_details 
    ON order_details.order_id_details = orders.order_id
GROUP BY 
    customers.company_name
ORDER BY 
    total DESC