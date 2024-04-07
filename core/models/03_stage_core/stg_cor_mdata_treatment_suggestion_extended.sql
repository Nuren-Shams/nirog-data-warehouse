{{-
    config(
        materialized = "table",
        tags = ["execute_daily", "mdata", "treatment_suggestion", "extended"]
    )
-}}

SELECT
    mdts.patient_id,
    mdts.collected_date,
    STRING_AGG(
        CONCAT(
            "Drug Name: ", COALESCE(CAST(rd.drug_code AS STRING), "-"), ", ",
            "Drug Dose: ", COALESCE(mdts.drug_dose, "-"), ", ",
            "Drug Frequency: ", COALESCE(mdts.frequency, "-"), ", ",
            "Drug Duration Value: ", COALESCE(mdts.drug_duration_value, "-"), ", ",
            "Unit: ", COALESCE(mdts.other_drug, "-")
        ), ";\n"
    ) AS prescribed_drugs

FROM
    {{ ref("bse_dbo_mdata_treatment_suggestion") }} AS mdts

LEFT JOIN {{ ref("bse_dbo_ref_drug") }} AS rd
    ON
        mdts.drug_id = rd.drug_id

GROUP BY
    mdts.patient_id,
    mdts.collected_date
