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
            "refprovisionaldiagnosiscategoriesforreporting.diseasecategory",
            "refprovisionaldiagnosiscategoriesforreporting.provisionaldiagnosiscode",
            "refprovisionaldiagnosiscategoriesforreporting.provisionaldiagnosisname",
            "refprovisionaldiagnosiscategoriesforreporting.refprovisionaldiagnosisid"
        ])
    }} AS ingestion_sk,
    refprovisionaldiagnosiscategoriesforreporting.diseasecategory,
    refprovisionaldiagnosiscategoriesforreporting.provisionaldiagnosiscode,
    refprovisionaldiagnosiscategoriesforreporting.provisionaldiagnosisname,
    refprovisionaldiagnosiscategoriesforreporting.refprovisionaldiagnosisid

FROM
    {{ source("bay_dbo", "refprovisionaldiagnosiscategoriesforreporting") }} AS refprovisionaldiagnosiscategoriesforreporting
