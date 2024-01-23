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
            "`refcancertreatment`.`cancertreatmentcode`",
            "`refcancertreatment`.`cancertreatmentid`",
            "`refcancertreatment`.`createdate`",
            "`refcancertreatment`.`createuser`",
            "`refcancertreatment`.`description`",
            "`refcancertreatment`.`orgid`",
            "`refcancertreatment`.`sortorder`",
            "`refcancertreatment`.`status`",
            "`refcancertreatment`.`updatedate`",
            "`refcancertreatment`.`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `refcancertreatment`.`cancertreatmentcode`,
    `refcancertreatment`.`cancertreatmentid`,
    `refcancertreatment`.`createdate`,
    `refcancertreatment`.`createuser`,
    `refcancertreatment`.`description`,
    `refcancertreatment`.`orgid`,
    `refcancertreatment`.`sortorder`,
    `refcancertreatment`.`status`,
    `refcancertreatment`.`updatedate`,
    `refcancertreatment`.`updateuser`

FROM
    {{ source("bay_dbo", "refcancertreatment") }} AS `refcancertreatment`
