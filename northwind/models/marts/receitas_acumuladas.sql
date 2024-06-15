WITH  orders AS (
    SELECT *
    FROM
        {{ref("stg_crm_orders")}}
),
order_details AS (
    SELECT *
    FROM
        {{ref("stg_crm_order_details")}}
),
ReceitasMensais AS (
    SELECT
       "Ano"
       ,"Mes"
       ,SUM(order_details.unit_price * quantity * (1.0 - order_details.discount)) AS Receita_Mensal
    FROM
        orders
    INNER JOIN
        order_details ON orders.order_id = order_details.order_id_details
    GROUP BY
        "Ano"
        ,"Mes"
),
ReceitasAcumuladas AS (
    SELECT
        "Ano"
        ,"Mes"
        ,Receita_Mensal
        ,SUM(Receita_Mensal) OVER (PARTITION BY "Ano" ORDER BY "Mes") AS Receita_YTD
    FROM
        ReceitasMensais
)
SELECT
    "Ano"
    ,"Mes"
    ,Receita_Mensal
    ,Receita_Mensal - LAG(Receita_Mensal) OVER (PARTITION BY "Ano" ORDER BY "Mes") AS Diferenca_Mensal
    ,Receita_YTD
    ,(Receita_Mensal - LAG(Receita_Mensal) OVER (PARTITION BY "Ano" ORDER BY "Mes")) / LAG(Receita_Mensal) OVER (PARTITION BY "Ano" ORDER BY "Mes") * 100 AS Percentual_Mudanca_Mensal
FROM
    ReceitasAcumuladas
ORDER BY
    "Ano", "Mes"