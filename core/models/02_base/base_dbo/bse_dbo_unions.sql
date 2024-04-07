{{-
    config(
        materialized = "table",
        tags = ["execute_daily"]
    )
-}}

SELECT
    SAFE_CAST(id AS INT64) AS union_id_int,
    IF(UPPER(name) IN ("NONE", ""), NULL, UPPER(name)) AS union_name,
    SAFE_CAST(upazilla_id AS INT64) AS upazila_id_int

FROM
    {{ ref("lan_dbo_unions") }}
