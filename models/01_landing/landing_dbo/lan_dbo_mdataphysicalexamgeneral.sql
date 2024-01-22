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
            "`anemiaseverity`",
            "`collectiondate`",
            "`createdate`",
            "`createuser`",
            "`cyanosis`",
            "`edemaseverity`",
            "`heartwithnad`",
            "`isheartwithnad`",
            "`islungswithnad`",
            "`islymphnodeswithpalpable`",
            "`jaundiceseverity`",
            "`lungswithnad`",
            "`lymphnodeswithpalpable`",
            "`lymphnodeswithpalpablesite`",
            "`lymphnodeswithpalpablesize`",
            "`mdphysicalexamgeneralid`",
            "`orgid`",
            "`othersymptom`",
            "`patientid`",
            "`status`",
            "`updatedate`",
            "`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `anemiaseverity`,
    `collectiondate`,
    `createdate`,
    `createuser`,
    `cyanosis`,
    `edemaseverity`,
    `heartwithnad`,
    `isheartwithnad`,
    `islungswithnad`,
    `islymphnodeswithpalpable`,
    `jaundiceseverity`,
    `lungswithnad`,
    `lymphnodeswithpalpable`,
    `lymphnodeswithpalpablesite`,
    `lymphnodeswithpalpablesize`,
    `mdphysicalexamgeneralid`,
    `orgid`,
    `othersymptom`,
    `patientid`,
    `status`,
    `updatedate`,
    `updateuser`

FROM
    {{ source("bay_dbo", "mdataphysicalexamgeneral") }}
