{{-
    config(
        materialized = "table",
        tags = ["execute_daily"]
    )
-}}

SELECT
    IF(UPPER(menstruationproductusagetimeid) IN ("NONE", ""), NULL, UPPER(menstruationproductusagetimeid)) AS menstruation_product_usage_time_id,
    IF(UPPER(menstruationproductusagetimecode) IN ("NONE", ""), NULL, UPPER(menstruationproductusagetimecode)) AS menstruation_product_usage_time_code,
    IF(UPPER(description) IN ("NONE", ""), NULL, UPPER(description)) AS description,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%E3S", createdate) AS created_at,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%E3S", updatedate) AS updated_at,

FROM
    {{ ref("lan_dbo_refmnstproductusagetime") }}

QUALIFY ROW_NUMBER() OVER(PARTITION BY menstruation_product_usage_time_id ORDER BY updated_at DESC) = 1
