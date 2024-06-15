WITH order_details AS (
    SELECT
        order_id AS "order_id_details"
        ,product_id
        ,unit_price
        ,quantity
        ,discount
    FROM
       {{ref("raw_crm__order_details")}} a

)
SELECT *
FROM
   order_details