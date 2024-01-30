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
            "`mdataprovisionaldiagnosis`.`category`",
            "`mdataprovisionaldiagnosis`.`collectiondate`",
            "`mdataprovisionaldiagnosis`.`createdate`",
            "`mdataprovisionaldiagnosis`.`createuser`",
            "`mdataprovisionaldiagnosis`.`diagnosisstatus`",
            "`mdataprovisionaldiagnosis`.`diseasegroupname`",
            "`mdataprovisionaldiagnosis`.`mdprovisionaldiagnosisid`",
            "`mdataprovisionaldiagnosis`.`orgid`",
            "`mdataprovisionaldiagnosis`.`otherprovisionaldiagnosis`",
            "`mdataprovisionaldiagnosis`.`patientid`",
            "`mdataprovisionaldiagnosis`.`provisionaldiagnosis`",
            "`mdataprovisionaldiagnosis`.`refdiseasegroupid`",
            "`mdataprovisionaldiagnosis`.`refprovisionaldiagnosisid`",
            "`mdataprovisionaldiagnosis`.`status`",
            "`mdataprovisionaldiagnosis`.`updatedate`",
            "`mdataprovisionaldiagnosis`.`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `mdataprovisionaldiagnosis`.`category`,
    `mdataprovisionaldiagnosis`.`collectiondate`,
    `mdataprovisionaldiagnosis`.`createdate`,
    `mdataprovisionaldiagnosis`.`createuser`,
    `mdataprovisionaldiagnosis`.`diagnosisstatus`,
    `mdataprovisionaldiagnosis`.`diseasegroupname`,
    `mdataprovisionaldiagnosis`.`mdprovisionaldiagnosisid`,
    `mdataprovisionaldiagnosis`.`orgid`,
    `mdataprovisionaldiagnosis`.`otherprovisionaldiagnosis`,
    `mdataprovisionaldiagnosis`.`patientid`,
    `mdataprovisionaldiagnosis`.`provisionaldiagnosis`,
    `mdataprovisionaldiagnosis`.`refdiseasegroupid`,
    `mdataprovisionaldiagnosis`.`refprovisionaldiagnosisid`,
    `mdataprovisionaldiagnosis`.`status`,
    `mdataprovisionaldiagnosis`.`updatedate`,
    `mdataprovisionaldiagnosis`.`updateuser`

FROM
    {{ source("bay_dbo", "mdataprovisionaldiagnosis") }} AS `mdataprovisionaldiagnosis`
