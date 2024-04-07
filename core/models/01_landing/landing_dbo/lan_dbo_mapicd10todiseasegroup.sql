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
            "`mapicd10todiseasegroup`.`createdate`",
            "`mapicd10todiseasegroup`.`createuser`",
            "`mapicd10todiseasegroup`.`mapicd10todiseasegroupid`",
            "`mapicd10todiseasegroup`.`orgid`",
            "`mapicd10todiseasegroup`.`refdiseasegroupid`",
            "`mapicd10todiseasegroup`.`refprovisionaldiagnosisid`",
            "`mapicd10todiseasegroup`.`status`",
            "`mapicd10todiseasegroup`.`updatedate`",
            "`mapicd10todiseasegroup`.`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `mapicd10todiseasegroup`.`createdate`,
    `mapicd10todiseasegroup`.`createuser`,
    `mapicd10todiseasegroup`.`mapicd10todiseasegroupid`,
    `mapicd10todiseasegroup`.`orgid`,
    `mapicd10todiseasegroup`.`refdiseasegroupid`,
    `mapicd10todiseasegroup`.`refprovisionaldiagnosisid`,
    `mapicd10todiseasegroup`.`status`,
    `mapicd10todiseasegroup`.`updatedate`,
    `mapicd10todiseasegroup`.`updateuser`

FROM
    {{ source("bay_dbo", "mapicd10todiseasegroup") }} AS `mapicd10todiseasegroup`
