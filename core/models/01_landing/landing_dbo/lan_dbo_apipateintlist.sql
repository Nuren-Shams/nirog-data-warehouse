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
            "`apipateintlist`.`bpstatus`",
            "`apipateintlist`.`conditionstatus`",
            "`apipateintlist`.`createdate`",
            "`apipateintlist`.`createuser`",
            "`apipateintlist`.`diseasename`",
            "`apipateintlist`.`glucosestatus`",
            "`apipateintlist`.`id`",
            "`apipateintlist`.`mdatapatientid`",
            "`apipateintlist`.`medicinestatus`",
            "`apipateintlist`.`orgid`",
            "`apipateintlist`.`patientid`",
            "`apipateintlist`.`registrationstatus`",
            "`apipateintlist`.`scheduledstatus`",
            "`apipateintlist`.`updatedate`",
            "`apipateintlist`.`updateuser`",
            "`apipateintlist`.`visitedstatus`"
        ])
    }} AS `ingestion_sk`,
    `apipateintlist`.`bpstatus`,
    `apipateintlist`.`conditionstatus`,
    `apipateintlist`.`createdate`,
    `apipateintlist`.`createuser`,
    `apipateintlist`.`diseasename`,
    `apipateintlist`.`glucosestatus`,
    `apipateintlist`.`id`,
    `apipateintlist`.`mdatapatientid`,
    `apipateintlist`.`medicinestatus`,
    `apipateintlist`.`orgid`,
    `apipateintlist`.`patientid`,
    `apipateintlist`.`registrationstatus`,
    `apipateintlist`.`scheduledstatus`,
    `apipateintlist`.`updatedate`,
    `apipateintlist`.`updateuser`,
    `apipateintlist`.`visitedstatus`

FROM
    {{ source("bay_dbo", "apipateintlist") }} AS `apipateintlist`
