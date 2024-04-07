{{-
    config(
        materialized = "table",
        tags = ["execute_daily"]
    )
-}}

SELECT
    SAFE_CAST(id AS INT64) AS district_id_int,
    CASE IF(UPPER(name) IN ("NONE", ""), NULL, UPPER(name))
        WHEN "COXSBAZAR" THEN "COX'S BAZAR"
        ELSE IF(UPPER(name) IN ("NONE", ""), NULL, UPPER(name))
    END AS district_name,
    SAFE_CAST(lat AS FLOAT64) AS district_lat,
    SAFE_CAST(lon AS FLOAT64) AS district_lon,
    SAFE_CAST(division_id AS INT64) AS division_id_int

FROM
    {{ ref("lan_dbo_districts") }}
