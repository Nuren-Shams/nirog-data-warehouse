{{-
    config(
        materialized = "table",
        tags = ["execute_daily", "mdata", "family_illness_history", "extended"]
    )
-}}

WITH mdata_family_illness_history AS (
    SELECT DISTINCT
        fih.patient_id,
        fih.collected_date,
        LOWER(other_ill_family_member) AS family_member,
        LOWER(ri.illness_code) AS illness_code,
        CONCAT(LOWER(other_ill_family_member), "_", LOWER(ri.illness_code)) AS family_member_illness_code

    FROM
        {{ ref("bse_dbo_mdata_family_illness_history") }} AS fih

    CROSS JOIN
        UNNEST(
            SPLIT(
                COALESCE(fih.other_ill_family_member, "OTHER"), ","
            )
        ) AS other_ill_family_member

    LEFT OUTER JOIN {{ ref("bse_dbo_ref_illness") }} AS ri
        ON
            fih.illness_id = ri.illness_id

    WHERE
        TRUE
        AND fih.illness_status = "YES"
)

, mdata_family_illness_history_agg AS (
    SELECT
        patient_id,
        collected_date,
        STRING_AGG(
            UPPER(
                CONCAT(family_member, ": ", illness_codes)
            ), ";\n" ORDER BY UPPER(
                CONCAT(family_member, ": ", illness_codes)
            ) ASC
        ) AS family_illness_history

    FROM (
        SELECT
            patient_id,
            collected_date,
            family_member,
            STRING_AGG(DISTINCT illness_code, ", " ORDER BY illness_code) AS illness_codes

        FROM
            mdata_family_illness_history
        
        GROUP BY
            patient_id,
            collected_date,
            family_member
    )

    GROUP BY
        patient_id,
        collected_date
)

, mdata_family_illness_history_pivoted AS (
    SELECT
        patient_id,
        collected_date,
        is_aunt_asthma,
        is_aunt_cancer,
        is_aunt_dengue,
        is_aunt_depression,
        is_aunt_dm,
        is_aunt_fracture_injury,
        is_aunt_hepatitis,
        is_aunt_hypertension,
        is_aunt_ihd,
        is_aunt_malaria,
        is_aunt_others,
        is_aunt_skin_disease,
        is_aunt_stroke,
        is_aunt_surgery,
        is_aunt_tb,
        is_aunt_typhoid,
        is_father_asthma,
        is_father_cancer,
        is_father_dengue,
        is_father_depression,
        is_father_dm,
        is_father_fracture_injury,
        is_father_hepatitis,
        is_father_hypertension,
        is_father_ihd,
        is_father_malaria,
        is_father_others,
        is_father_skin_disease,
        is_father_stroke,
        is_father_surgery,
        is_father_tb,
        is_father_typhoid,
        is_grandparents_asthma,
        is_grandparents_cancer,
        is_grandparents_dengue,
        is_grandparents_depression,
        is_grandparents_dm,
        is_grandparents_fracture_injury,
        is_grandparents_hepatitis,
        is_grandparents_hypertension,
        is_grandparents_ihd,
        is_grandparents_malaria,
        is_grandparents_others,
        is_grandparents_skin_disease,
        is_grandparents_stroke,
        is_grandparents_surgery,
        is_grandparents_tb,
        is_grandparents_typhoid,
        is_mother_asthma,
        is_mother_cancer,
        is_mother_dengue,
        is_mother_depression,
        is_mother_dm,
        is_mother_fracture_injury,
        is_mother_hepatitis,
        is_mother_hypertension,
        is_mother_ihd,
        is_mother_malaria,
        is_mother_others,
        is_mother_skin_disease,
        is_mother_stroke,
        is_mother_surgery,
        is_mother_tb,
        is_mother_typhoid,
        is_other_asthma,
        is_other_cancer,
        is_other_dengue,
        is_other_depression,
        is_other_dm,
        is_other_fracture_injury,
        is_other_hepatitis,
        is_other_hypertension,
        is_other_ihd,
        is_other_malaria,
        is_other_others,
        is_other_skin_disease,
        is_other_stroke,
        is_other_surgery,
        is_other_tb,
        is_other_typhoid,
        is_siblings_asthma,
        is_siblings_cancer,
        is_siblings_dengue,
        is_siblings_depression,
        is_siblings_dm,
        is_siblings_fracture_injury,
        is_siblings_hepatitis,
        is_siblings_hypertension,
        is_siblings_ihd,
        is_siblings_malaria,
        is_siblings_others,
        is_siblings_skin_disease,
        is_siblings_stroke,
        is_siblings_surgery,
        is_siblings_tb,
        is_siblings_typhoid

    FROM
        mdata_family_illness_history

    PIVOT (
        ANY_VALUE(TRUE) AS `is` FOR family_member_illness_code IN (
            "aunt_asthma",
            "aunt_cancer",
            "aunt_dengue",
            "aunt_depression",
            "aunt_dm",
            "aunt_fracture_injury",
            "aunt_hepatitis",
            "aunt_hypertension",
            "aunt_ihd",
            "aunt_malaria",
            "aunt_others",
            "aunt_skin_disease",
            "aunt_stroke",
            "aunt_surgery",
            "aunt_tb",
            "aunt_typhoid",
            "father_asthma",
            "father_cancer",
            "father_dengue",
            "father_depression",
            "father_dm",
            "father_fracture_injury",
            "father_hepatitis",
            "father_hypertension",
            "father_ihd",
            "father_malaria",
            "father_others",
            "father_skin_disease",
            "father_stroke",
            "father_surgery",
            "father_tb",
            "father_typhoid",
            "grandparents_asthma",
            "grandparents_cancer",
            "grandparents_dengue",
            "grandparents_depression",
            "grandparents_dm",
            "grandparents_fracture_injury",
            "grandparents_hepatitis",
            "grandparents_hypertension",
            "grandparents_ihd",
            "grandparents_malaria",
            "grandparents_others",
            "grandparents_skin_disease",
            "grandparents_stroke",
            "grandparents_surgery",
            "grandparents_tb",
            "grandparents_typhoid",
            "mother_asthma",
            "mother_cancer",
            "mother_dengue",
            "mother_depression",
            "mother_dm",
            "mother_fracture_injury",
            "mother_hepatitis",
            "mother_hypertension",
            "mother_ihd",
            "mother_malaria",
            "mother_others",
            "mother_skin_disease",
            "mother_stroke",
            "mother_surgery",
            "mother_tb",
            "mother_typhoid",
            "other_asthma",
            "other_cancer",
            "other_dengue",
            "other_depression",
            "other_dm",
            "other_fracture_injury",
            "other_hepatitis",
            "other_hypertension",
            "other_ihd",
            "other_malaria",
            "other_others",
            "other_skin_disease",
            "other_stroke",
            "other_surgery",
            "other_tb",
            "other_typhoid",
            "siblings_asthma",
            "siblings_cancer",
            "siblings_dengue",
            "siblings_depression",
            "siblings_dm",
            "siblings_fracture_injury",
            "siblings_hepatitis",
            "siblings_hypertension",
            "siblings_ihd",
            "siblings_malaria",
            "siblings_others",
            "siblings_skin_disease",
            "siblings_stroke",
            "siblings_surgery",
            "siblings_tb",
            "siblings_typhoid"
        )
    )
)

SELECT
    mdfihp.patient_id,
    mdfihp.collected_date,
    mdfihp.is_aunt_asthma,
    mdfihp.is_aunt_cancer,
    mdfihp.is_aunt_dengue,
    mdfihp.is_aunt_depression,
    mdfihp.is_aunt_dm,
    mdfihp.is_aunt_fracture_injury,
    mdfihp.is_aunt_hepatitis,
    mdfihp.is_aunt_hypertension,
    mdfihp.is_aunt_ihd,
    mdfihp.is_aunt_malaria,
    mdfihp.is_aunt_others,
    mdfihp.is_aunt_skin_disease,
    mdfihp.is_aunt_stroke,
    mdfihp.is_aunt_surgery,
    mdfihp.is_aunt_tb,
    mdfihp.is_aunt_typhoid,
    mdfihp.is_father_asthma,
    mdfihp.is_father_cancer,
    mdfihp.is_father_dengue,
    mdfihp.is_father_depression,
    mdfihp.is_father_dm,
    mdfihp.is_father_fracture_injury,
    mdfihp.is_father_hepatitis,
    mdfihp.is_father_hypertension,
    mdfihp.is_father_ihd,
    mdfihp.is_father_malaria,
    mdfihp.is_father_others,
    mdfihp.is_father_skin_disease,
    mdfihp.is_father_stroke,
    mdfihp.is_father_surgery,
    mdfihp.is_father_tb,
    mdfihp.is_father_typhoid,
    mdfihp.is_grandparents_asthma,
    mdfihp.is_grandparents_cancer,
    mdfihp.is_grandparents_dengue,
    mdfihp.is_grandparents_depression,
    mdfihp.is_grandparents_dm,
    mdfihp.is_grandparents_fracture_injury,
    mdfihp.is_grandparents_hepatitis,
    mdfihp.is_grandparents_hypertension,
    mdfihp.is_grandparents_ihd,
    mdfihp.is_grandparents_malaria,
    mdfihp.is_grandparents_others,
    mdfihp.is_grandparents_skin_disease,
    mdfihp.is_grandparents_stroke,
    mdfihp.is_grandparents_surgery,
    mdfihp.is_grandparents_tb,
    mdfihp.is_grandparents_typhoid,
    mdfihp.is_mother_asthma,
    mdfihp.is_mother_cancer,
    mdfihp.is_mother_dengue,
    mdfihp.is_mother_depression,
    mdfihp.is_mother_dm,
    mdfihp.is_mother_fracture_injury,
    mdfihp.is_mother_hepatitis,
    mdfihp.is_mother_hypertension,
    mdfihp.is_mother_ihd,
    mdfihp.is_mother_malaria,
    mdfihp.is_mother_others,
    mdfihp.is_mother_skin_disease,
    mdfihp.is_mother_stroke,
    mdfihp.is_mother_surgery,
    mdfihp.is_mother_tb,
    mdfihp.is_mother_typhoid,
    mdfihp.is_other_asthma,
    mdfihp.is_other_cancer,
    mdfihp.is_other_dengue,
    mdfihp.is_other_depression,
    mdfihp.is_other_dm,
    mdfihp.is_other_fracture_injury,
    mdfihp.is_other_hepatitis,
    mdfihp.is_other_hypertension,
    mdfihp.is_other_ihd,
    mdfihp.is_other_malaria,
    mdfihp.is_other_others,
    mdfihp.is_other_skin_disease,
    mdfihp.is_other_stroke,
    mdfihp.is_other_surgery,
    mdfihp.is_other_tb,
    mdfihp.is_other_typhoid,
    mdfihp.is_siblings_asthma,
    mdfihp.is_siblings_cancer,
    mdfihp.is_siblings_dengue,
    mdfihp.is_siblings_depression,
    mdfihp.is_siblings_dm,
    mdfihp.is_siblings_fracture_injury,
    mdfihp.is_siblings_hepatitis,
    mdfihp.is_siblings_hypertension,
    mdfihp.is_siblings_ihd,
    mdfihp.is_siblings_malaria,
    mdfihp.is_siblings_others,
    mdfihp.is_siblings_skin_disease,
    mdfihp.is_siblings_stroke,
    mdfihp.is_siblings_surgery,
    mdfihp.is_siblings_tb,
    mdfihp.is_siblings_typhoid,
    mdfiha.family_illness_history

FROM
    mdata_family_illness_history_pivoted AS mdfihp

LEFT OUTER JOIN
    mdata_family_illness_history_agg AS mdfiha 
    ON
        mdfihp.patient_id = mdfiha.patient_id
        AND mdfihp.collected_date = mdfiha.collected_date