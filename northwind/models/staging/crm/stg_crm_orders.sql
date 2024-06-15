WITH orders AS (
    SELECT
        A.*
        ,EXTRACT(YEAR FROM order_date) AS "Ano"
        ,EXTRACT(MONTH FROM order_date) AS "Mes"
    FROM
       {{ref("raw_crm__orders")}} A
)
SELECT *
FROM
   orders