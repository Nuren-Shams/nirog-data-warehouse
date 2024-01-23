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
            "`organization`.`address1`",
            "`organization`.`address2`",
            "`organization`.`city`",
            "`organization`.`country`",
            "`organization`.`createdate`",
            "`organization`.`createuser`",
            "`organization`.`emailaddress`",
            "`organization`.`orgcode`",
            "`organization`.`orgid`",
            "`organization`.`orgname`",
            "`organization`.`status`",
            "`organization`.`updatedate`",
            "`organization`.`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `organization`.`address1`,
    `organization`.`address2`,
    `organization`.`city`,
    `organization`.`country`,
    `organization`.`createdate`,
    `organization`.`createuser`,
    `organization`.`emailaddress`,
    `organization`.`orgcode`,
    `organization`.`orgid`,
    `organization`.`orgname`,
    `organization`.`status`,
    `organization`.`updatedate`,
    `organization`.`updateuser`

FROM
    {{ source("bay_dbo", "organization") }} AS `organization`
