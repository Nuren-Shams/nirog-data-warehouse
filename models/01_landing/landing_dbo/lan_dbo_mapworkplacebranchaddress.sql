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
            "`addressid`",
            "`createdate`",
            "`createuser`",
            "`mappingworkplacebranchaddressid`",
            "`orgid`",
            "`status`",
            "`updatedate`",
            "`updateuser`",
            "`workplacebranchid`"
        ])
    }} AS `ingestion_sk`,
    `addressid`,
    `createdate`,
    `createuser`,
    `mappingworkplacebranchaddressid`,
    `orgid`,
    `status`,
    `updatedate`,
    `updateuser`,
    `workplacebranchid`

FROM
    {{ source("bay_dbo", "mapworkplacebranchaddress") }}
