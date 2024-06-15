WITH orders AS (
    SELECT *
    FROM
        {{ref("stg_crm_orders")}}
    WHERE
        "Ano" = '1997'
),
order_details as (
    SELECT *
    FROM
        {{ref("stg_crm_order_details")}}
)
SELECT
     cast(SUM((OD.unit_price) * OD.quantity * (1.0 - OD.discount)) as decimal(15,2)) AS "total_revenues_1997"
FROM
   order_details OD
INNER JOIN orders O
ON OD.order_id_details = O.order_id