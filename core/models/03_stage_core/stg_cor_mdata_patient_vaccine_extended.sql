{{-
    config(
        materialized = "table",
        tags = ["execute_daily", "mdata", "patient_vaccine", "extended"]
    )
-}}

SELECT
    mdpv.patient_id
    , mdpv.collected_date
    , LOGICAL_OR(UPPER(rv.vaccine_code) = "BCG") AS is_vac_bcg
    , LOGICAL_OR(UPPER(rv.vaccine_code) = "PENTAVALENT") AS is_vac_pentavalent
    , LOGICAL_OR(UPPER(rv.vaccine_code) = "PCV") AS is_vac_pcv
    , LOGICAL_OR(UPPER(rv.vaccine_code) = "OPV") AS is_vac_opv
    , LOGICAL_OR(UPPER(rv.vaccine_code) = "MR") AS is_vac_mrs
    , LOGICAL_OR(UPPER(rv.vaccine_code) = "MEASLES") AS is_vac_measles
    , LOGICAL_OR(UPPER(rv.vaccine_code) = "CHICKEN POX") AS is_vac_chicken_pox
    , LOGICAL_OR(UPPER(rv.vaccine_code) = "COVID-19") AS is_vac_covid_19

FROM
    {{ ref("bse_dbo_mdata_patient_vaccine") }} AS mdpv

    LEFT JOIN {{ ref("bse_dbo_ref_vaccine") }} AS rv
        ON
            mdpv.vaccine_id = rv.vaccine_id

GROUP BY
    mdpv.patient_id
    , mdpv.collected_date
