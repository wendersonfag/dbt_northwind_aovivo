WITH renamed as (
    SELECT *
    FROM
       {{ref("raw_crm__nova_tabela")}}
)
SELECT 
     *
FROM
   renamed