{{-
    config(
        materialized = "table",
        tags = ["execute_daily", "mdata", "patient_illness_history", "extended"]
    )
-}}


SELECT
    pih.patient_id,
    pih.collected_date,
    ri.illness_code

FROM
    {{ ref("bse_dbo_mdata_patient_illness_history") }} AS pih

LEFT OUTER JOIN {{ ref("bse_dbo_ref_illness") }} AS ri
    ON
        pih.illness_id = ri.illness_id

WHERE
    TRUE
    AND pih.illness_status = "YES"
