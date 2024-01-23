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
            "`refconfiguration`.`configid`",
            "`refconfiguration`.`configkey`",
            "`refconfiguration`.`configvalue`",
            "`refconfiguration`.`createdate`",
            "`refconfiguration`.`createuser`",
            "`refconfiguration`.`description`",
            "`refconfiguration`.`orgid`",
            "`refconfiguration`.`status`",
            "`refconfiguration`.`updatedate`",
            "`refconfiguration`.`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `refconfiguration`.`configid`,
    `refconfiguration`.`configkey`,
    `refconfiguration`.`configvalue`,
    `refconfiguration`.`createdate`,
    `refconfiguration`.`createuser`,
    `refconfiguration`.`description`,
    `refconfiguration`.`orgid`,
    `refconfiguration`.`status`,
    `refconfiguration`.`updatedate`,
    `refconfiguration`.`updateuser`

FROM
    {{ source("bay_dbo", "refconfiguration") }} AS `refconfiguration`
