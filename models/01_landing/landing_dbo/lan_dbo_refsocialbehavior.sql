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
            "`refsocialbehavior`.`createdate`",
            "`refsocialbehavior`.`createuser`",
            "`refsocialbehavior`.`description`",
            "`refsocialbehavior`.`orgid`",
            "`refsocialbehavior`.`socialbehaviorcode`",
            "`refsocialbehavior`.`socialbehaviorid`",
            "`refsocialbehavior`.`sortorder`",
            "`refsocialbehavior`.`status`",
            "`refsocialbehavior`.`updatedate`",
            "`refsocialbehavior`.`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `refsocialbehavior`.`createdate`,
    `refsocialbehavior`.`createuser`,
    `refsocialbehavior`.`description`,
    `refsocialbehavior`.`orgid`,
    `refsocialbehavior`.`socialbehaviorcode`,
    `refsocialbehavior`.`socialbehaviorid`,
    `refsocialbehavior`.`sortorder`,
    `refsocialbehavior`.`status`,
    `refsocialbehavior`.`updatedate`,
    `refsocialbehavior`.`updateuser`

FROM
    {{ source("bay_dbo", "refsocialbehavior") }} AS `refsocialbehavior`
