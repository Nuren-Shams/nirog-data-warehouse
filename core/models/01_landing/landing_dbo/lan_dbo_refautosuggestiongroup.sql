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
            "`refautosuggestiongroup`.`createdate`",
            "`refautosuggestiongroup`.`createuser`",
            "`refautosuggestiongroup`.`description`",
            "`refautosuggestiongroup`.`orgid`",
            "`refautosuggestiongroup`.`refautosuggestiongroupcode`",
            "`refautosuggestiongroup`.`refautosuggestiongroupid`",
            "`refautosuggestiongroup`.`sortorder`",
            "`refautosuggestiongroup`.`status`",
            "`refautosuggestiongroup`.`updatedate`",
            "`refautosuggestiongroup`.`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `refautosuggestiongroup`.`createdate`,
    `refautosuggestiongroup`.`createuser`,
    `refautosuggestiongroup`.`description`,
    `refautosuggestiongroup`.`orgid`,
    `refautosuggestiongroup`.`refautosuggestiongroupcode`,
    `refautosuggestiongroup`.`refautosuggestiongroupid`,
    `refautosuggestiongroup`.`sortorder`,
    `refautosuggestiongroup`.`status`,
    `refautosuggestiongroup`.`updatedate`,
    `refautosuggestiongroup`.`updateuser`

FROM
    {{ source("bay_dbo", "refautosuggestiongroup") }} AS `refautosuggestiongroup`
