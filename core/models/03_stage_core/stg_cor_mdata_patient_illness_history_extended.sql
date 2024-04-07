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
            pih.illness_id = ri.illness_id

    WHERE
        TRUE
        AND pih.illness_status = "YES"
),

mdata_patient_illness_history_agg AS (
    SELECT
        pih.patient_id,
        pih.collected_date,
        STRING_AGG(DISTINCT UPPER(ri.illness_code), ", " ORDER BY UPPER(ri.illness_code)) AS patient_illness_history

    FROM
        {{ ref("bse_dbo_mdata_patient_illness_history") }} AS pih

    LEFT OUTER JOIN {{ ref("bse_dbo_ref_illness") }} AS ri
        ON
            pih.illness_id = ri.illness_id

    WHERE
        TRUE
        AND pih.illness_status = "YES"

    GROUP BY
        pih.patient_id,
        pih.collected_date
),

mdata_patient_illness_history_pivoted AS (
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

    PIVOT (
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
)

SELECT
    mdpihp.patient_id,
    mdpihp.collected_date,
    mdpihp.is_asthma,
    mdpihp.is_cancer,
    mdpihp.is_dengue,
    mdpihp.is_depression,
    mdpihp.is_dm,
    mdpihp.is_fracture_injury,
    mdpihp.is_hepatitis,
    mdpihp.is_hypertension,
    mdpihp.is_ihd,
    mdpihp.is_malaria,
    mdpihp.is_others,
    mdpihp.is_skin_disease,
    mdpihp.is_stroke,
    mdpihp.is_surgery,
    mdpihp.is_tb,
    mdpihp.is_typhoid,
    mdpiha.patient_illness_history

FROM
    mdata_patient_illness_history_pivoted AS mdpihp
LEFT JOIN
    mdata_patient_illness_history_agg AS mdpiha
    ON
        mdpihp.patient_id = mdpiha.patient_id
        AND mdpihp.collected_date = mdpiha.collected_date
