{{-
    config(
        materialized = "incremental",
        unique_key = "ingestion_sk",
        tags = ["execute_daily"]
    )
-}}


SELECT
    {{
        dbt_utils.generate_surrogate_key([
            "`diseasecategory`",
            "`provisionaldiagnosiscode`",
            "`provisionaldiagnosisname`",
            "`refprovisionaldiagnosisid`"
        ])
    }} AS `ingestion_sk`,
    `diseasecategory`,
    `provisionaldiagnosiscode`,
    `provisionaldiagnosisname`,
    `refprovisionaldiagnosisid`

FROM
    {{ source("bay_dbo", "refprovisionaldiagnosiscategoriesforreporting") }}
