{{
    config(
        materialized = "table"
    )
}}

SELECT *
FROM {{source("landing", "address")}}