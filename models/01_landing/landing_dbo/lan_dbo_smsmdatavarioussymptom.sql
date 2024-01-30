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
            "`smsmdatavarioussymptom`.`collectiondate`",
            "`smsmdatavarioussymptom`.`createdate`",
            "`smsmdatavarioussymptom`.`ispulled`",
            "`smsmdatavarioussymptom`.`mdatatype`",
            "`smsmdatavarioussymptom`.`orgid`",
            "`smsmdatavarioussymptom`.`patientid`",
            "`smsmdatavarioussymptom`.`smsmdvarioussymptomid`"
        ])
    }} AS `ingestion_sk`,
    `smsmdatavarioussymptom`.`collectiondate`,
    `smsmdatavarioussymptom`.`createdate`,
    `smsmdatavarioussymptom`.`ispulled`,
    `smsmdatavarioussymptom`.`mdatatype`,
    `smsmdatavarioussymptom`.`orgid`,
    `smsmdatavarioussymptom`.`patientid`,
    `smsmdatavarioussymptom`.`smsmdvarioussymptomid`

FROM
    {{ source("bay_dbo", "smsmdatavarioussymptom") }} AS `smsmdatavarioussymptom`
