{{-
    config(
        materialized = "table",
        tags = ["execute_daily", "mdata", "provisional_diagnosis", "extended"]
    )
-}}

SELECT
    patient_id,
    collected_date,
    REGEXP_REPLACE(
        TRIM(
            STRING_AGG(
                CONCAT(
                    "Provisional Diagnosis: ", COALESCE(CAST(mdpd.provisional_diagnosis AS STRING), "-"), ", ",
                    "Other Provisional Diagnosis: ", COALESCE(mdpd.other_provisional_diagnosis, "-"), ", ",
                    "Diagnosis Status: ", COALESCE(mdpd.diagnosis_status, "-")
                ), ";\n"
            )
         ), R"\s+", " "
     ) AS provisional_diagnosis_details

FROM
    {{ ref("bse_dbo_mdata_provisional_diagnosis") }} AS mdpd

GROUP BY
    patient_id,
    collected_date
