{{-
    config(
        materialized = "table",
        tags = ["execute_daily"]
    )
-}}

SELECT
    SAFE_CAST(id AS INT64) AS union_id_int,
    UPPER(name) AS union_name,
    SAFE_CAST(upazilla_id AS INT64) AS upazilla_id_int

FROM
    {{ ref("lan_dbo_unions") }}
