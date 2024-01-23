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
            "`refvaccineadult`.`createdate`",
            "`refvaccineadult`.`createuser`",
            "`refvaccineadult`.`description`",
            "`refvaccineadult`.`instruction`",
            "`refvaccineadult`.`orgid`",
            "`refvaccineadult`.`sortorder`",
            "`refvaccineadult`.`status`",
            "`refvaccineadult`.`updatedate`",
            "`refvaccineadult`.`updateuser`",
            "`refvaccineadult`.`vaccinecode`",
            "`refvaccineadult`.`vaccinedosegroupid`",
            "`refvaccineadult`.`vaccinedosenumber`",
            "`refvaccineadult`.`vaccinedosetype`",
            "`refvaccineadult`.`vaccineid`",
            "`refvaccineadult`.`vaccineprovidertype`"
        ])
    }} AS `ingestion_sk`,
    `refvaccineadult`.`createdate`,
    `refvaccineadult`.`createuser`,
    `refvaccineadult`.`description`,
    `refvaccineadult`.`instruction`,
    `refvaccineadult`.`orgid`,
    `refvaccineadult`.`sortorder`,
    `refvaccineadult`.`status`,
    `refvaccineadult`.`updatedate`,
    `refvaccineadult`.`updateuser`,
    `refvaccineadult`.`vaccinecode`,
    `refvaccineadult`.`vaccinedosegroupid`,
    `refvaccineadult`.`vaccinedosenumber`,
    `refvaccineadult`.`vaccinedosetype`,
    `refvaccineadult`.`vaccineid`,
    `refvaccineadult`.`vaccineprovidertype`

FROM
    {{ source("bay_dbo", "refvaccineadult") }} AS `refvaccineadult`
