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
            "`mapworkplaceaddress`.`addressid`",
            "`mapworkplaceaddress`.`createdate`",
            "`mapworkplaceaddress`.`createuser`",
            "`mapworkplaceaddress`.`mappingworkplaceaddressid`",
            "`mapworkplaceaddress`.`orgid`",
            "`mapworkplaceaddress`.`status`",
            "`mapworkplaceaddress`.`updatedate`",
            "`mapworkplaceaddress`.`updateuser`",
            "`mapworkplaceaddress`.`workplaceid`"
        ])
    }} AS `ingestion_sk`,
    `mapworkplaceaddress`.`addressid`,
    `mapworkplaceaddress`.`createdate`,
    `mapworkplaceaddress`.`createuser`,
    `mapworkplaceaddress`.`mappingworkplaceaddressid`,
    `mapworkplaceaddress`.`orgid`,
    `mapworkplaceaddress`.`status`,
    `mapworkplaceaddress`.`updatedate`,
    `mapworkplaceaddress`.`updateuser`,
    `mapworkplaceaddress`.`workplaceid`

FROM
    {{ source("bay_dbo", "mapworkplaceaddress") }} AS `mapworkplaceaddress`
