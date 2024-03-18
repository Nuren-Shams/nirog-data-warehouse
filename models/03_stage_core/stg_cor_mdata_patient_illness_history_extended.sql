{{-
    config(
        materialized = "table",
        tags = ["execute_daily", "mdata", "patient_illness_history", "extended"]
    )
-}}

WITH mdata_patient_illness_history AS (
    SELECT
        pih.patient_id,
        pih.collected_date,
        LOWER(ri.illness_code) AS illness_code

    FROM
        {{ ref("bse_dbo_mdata_patient_illness_history") }} AS pih

        LEFT OUTER JOIN {{ ref("bse_dbo_ref_illness") }} AS ri
            ON
                ri.illness_id = pih.illness_id

    WHERE TRUE
        AND pih.illness_status = "YES"
)

SELECT
    patient_id,
    collected_date,
    is_asthma,
    is_cancer,
    is_dengue,
    is_depression,
    is_dm,
    is_fracture_injury,
    is_hepatitis,
    is_hypertension,
    is_ihd,
    is_malaria,
    is_others,
    is_skin_disease,
    is_stroke,
    is_surgery,
    is_tb,
    is_typhoid

FROM
    mdata_patient_illness_history

    PIVOT(
        ANY_VALUE(TRUE) AS `is` FOR illness_code IN (
            "asthma",
            "cancer",
            "dengue",
            "depression",
            "dm",
            "fracture_injury",
            "hepatitis",
            "hypertension",
            "ihd",
            "malaria",
            "others",
            "skin_disease",
            "stroke",
            "surgery",
            "tb",
            "typhoid"
        )
    )
