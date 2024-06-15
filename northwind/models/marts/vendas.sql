--fails, sysnc_all_columns, append_new_columns
{{
    config(
        materialized='incremental',
        unique_key='category_id',
        on_schema_change='append_new_columns' 
    )
}}

WITH vendas as (
    SELECT 
        A.*
        ,'coluna nova' as "teste2"
    FROM
        {{ref("stg_crm__nova_tabela")}}  A
)
SELECT *
FROM
    vendas

{% if is_incremental() %}

   -- Assuming Postgres database
   where updated_at > (SELECT max(updated_at) FROM {{ this }})

{% endif %}