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
            "`refeducation`.`createdate`",
            "`refeducation`.`createuser`",
            "`refeducation`.`description`",
            "`refeducation`.`educationcode`",
            "`refeducation`.`educationid`",
            "`refeducation`.`orgid`",
            "`refeducation`.`sortorder`",
            "`refeducation`.`status`",
            "`refeducation`.`updatedate`",
            "`refeducation`.`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `refeducation`.`createdate`,
    `refeducation`.`createuser`,
    `refeducation`.`description`,
    `refeducation`.`educationcode`,
    `refeducation`.`educationid`,
    `refeducation`.`orgid`,
    `refeducation`.`sortorder`,
    `refeducation`.`status`,
    `refeducation`.`updatedate`,
    `refeducation`.`updateuser`

FROM
    {{ source("bay_dbo", "refeducation") }} AS `refeducation`
