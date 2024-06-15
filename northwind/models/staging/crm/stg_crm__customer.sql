WITH customers AS (
    SELECT *
    FROM
        {{ref("raw_crm__customer")}}
)
SELECT *
FROM
    customers