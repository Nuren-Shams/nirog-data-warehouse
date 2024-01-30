{{-
    config(
        materialized = "table",
        tags = ["execute_daily"]
    )
-}}

SELECT
    SAFE_CAST(id AS INT64) AS upazila_id_int,
    IF(UPPER(name) IN ("NONE", ""), NULL, UPPER(name)) AS upazila_name,
    SAFE_CAST(district_id AS INT64) AS district_id_int

FROM
    {{ ref("lan_dbo_upazilas") }}
