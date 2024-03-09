{{-
    config(
        materialized = "table",
        tags = ["execute_daily", "mdata", "patient_cc_details", "extended"]
    )
-}}

SELECT
    mdpccd.patient_id
    , mdpccd.collected_date
    , STRING_AGG(
        CONCAT(
            "Chief Complain: ", COALESCE(CAST(mdpccd.chief_complain AS STRING), "-"), ", "
            , "Duration: ", COALESCE(rd.duration_code, "-"), ", "
            , "Duration: ", COALESCE(CAST(mdpccd.cc_duration_value AS STRING), "-"), ", "
            , "Nature: ", COALESCE(mdpccd.nature, "-")
        ), ";\n"
    ) AS chief_complain_with_duration

FROM
    {{ ref("bse_dbo_mdata_patient_cc_details") }} AS mdpccd

    LEFT JOIN {{ ref("bse_dbo_ref_duration") }} AS rd
        ON
            mdpccd.duration_id = rd.duration_id

GROUP BY
    mdpccd.patient_id
    , mdpccd.collected_date
