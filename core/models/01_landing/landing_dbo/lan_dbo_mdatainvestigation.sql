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
            "`mdatainvestigation`.`collectiondate`",
            "`mdatainvestigation`.`createdate`",
            "`mdatainvestigation`.`createuser`",
            "`mdatainvestigation`.`instruction`",
            "`mdatainvestigation`.`investigationid`",
            "`mdatainvestigation`.`mdinvestigationid`",
            "`mdatainvestigation`.`orgid`",
            "`mdatainvestigation`.`otherinvestigation`",
            "`mdatainvestigation`.`patientid`",
            "`mdatainvestigation`.`positivenegativestatus`",
            "`mdatainvestigation`.`status`",
            "`mdatainvestigation`.`updatedate`",
            "`mdatainvestigation`.`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `mdatainvestigation`.`collectiondate`,
    `mdatainvestigation`.`createdate`,
    `mdatainvestigation`.`createuser`,
    `mdatainvestigation`.`instruction`,
    `mdatainvestigation`.`investigationid`,
    `mdatainvestigation`.`mdinvestigationid`,
    `mdatainvestigation`.`orgid`,
    `mdatainvestigation`.`otherinvestigation`,
    `mdatainvestigation`.`patientid`,
    `mdatainvestigation`.`positivenegativestatus`,
    `mdatainvestigation`.`status`,
    `mdatainvestigation`.`updatedate`,
    `mdatainvestigation`.`updateuser`

FROM
    {{ source("bay_dbo", "mdatainvestigation") }} AS `mdatainvestigation`
