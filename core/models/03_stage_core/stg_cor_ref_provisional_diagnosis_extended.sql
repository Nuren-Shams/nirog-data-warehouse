{{-
    config(
        materialized = "table",
        tags = ["execute_daily", "ref", "provisional_diagnosis"]
    )
-}}

SELECT
    rpd.ref_provisional_diagnosis_id,
    rpd.ref_provisional_diagnosis_group_id,
    REGEXP_REPLACE(rpd.provisional_diagnosis_code, R"\s+", " ") AS provisional_diagnosis_code,
    REGEXP_REPLACE(rpd.provisional_diagnosis_name, R"\s+", " ") AS provisional_diagnosis_name,
    CONCAT(
        REGEXP_REPLACE(rpd.provisional_diagnosis_code, R"\s+", " "),
        " - ",
        REGEXP_REPLACE(rpd.provisional_diagnosis_name, R"\s+", " ")
    ) AS provisional_diagnosis

FROM
    {{ ref("bse_dbo_ref_provisional_diagnosis") }} AS rpd
