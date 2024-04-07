{{-
    config(
        materialized = "table",
        tags = ["execute_daily"]
    )
-}}

SELECT
    IF(UPPER(menstruationproductid) IN ("NONE", ""), NULL, UPPER(menstruationproductid)) AS menstruation_product_id,
    IF(UPPER(menstruationproductcode) IN ("NONE", ""), NULL, UPPER(menstruationproductcode)) AS menstruation_product_code,
    IF(UPPER(description) IN ("NONE", ""), NULL, UPPER(description)) AS description,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%E3S", createdate) AS created_at,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%E3S", updatedate) AS updated_at

FROM
    {{ ref("lan_dbo_refmenstruationproduct") }}

QUALIFY ROW_NUMBER() OVER (PARTITION BY menstruation_product_id ORDER BY updated_at DESC) = 1
