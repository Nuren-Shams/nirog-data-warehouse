{{-
    config(
        materialized = "table",
        tags = ["execute_daily", "mdata", "physical_finding", "extended"]
    )
-}}

SELECT
    patient_id,
    collected_date,
    STRING_AGG(DISTINCT physical_finding, ", " ORDER BY physical_finding ASC) AS physical_findings

FROM
    {{ ref("bse_dbo_mdata_physical_finding") }}

GROUP BY
    patient_id,
    collected_date
