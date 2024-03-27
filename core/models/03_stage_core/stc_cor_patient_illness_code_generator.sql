{{-
    config(
        materialized = "table",
        tags = ["execute_daily", "mdata", "patient_illness_code", "generator"]
    )
-}}

SELECT
    STRING_AGG(LOWER(CONCAT("is_", illness_code)), ",\n" ORDER BY illness_code ASC) AS patient_illness_code_id,
    STRING_AGG(LOWER(CONCAT('"', illness_code, '"')), ",\n" ORDER BY illness_code ASC) AS patient_illness_code_value

FROM
    {{ ref("bse_dbo_ref_illness") }}
