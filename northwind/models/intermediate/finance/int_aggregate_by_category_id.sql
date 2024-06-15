WITH int_aggrefate_by_category_id AS (
    SELECT
         category_id,
         count(*)
    FROM
       {{ref("stg_crm__nova_tabela")}}
    GROUP BY 
        category_id
)
SELECT *
FROM
    int_aggrefate_by_category_id