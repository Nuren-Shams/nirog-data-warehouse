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
            "`refdrug`.`createdate`",
            "`refdrug`.`createuser`",
            "`refdrug`.`description`",
            "`refdrug`.`drugcode`",
            "`refdrug`.`drugdose`",
            "`refdrug`.`drugformid`",
            "`refdrug`.`druggroupid`",
            "`refdrug`.`drugid`",
            "`refdrug`.`orgid`",
            "`refdrug`.`sortorder`",
            "`refdrug`.`status`",
            "`refdrug`.`updatedate`",
            "`refdrug`.`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `refdrug`.`createdate`,
    `refdrug`.`createuser`,
    `refdrug`.`description`,
    `refdrug`.`drugcode`,
    `refdrug`.`drugdose`,
    `refdrug`.`drugformid`,
    `refdrug`.`druggroupid`,
    `refdrug`.`drugid`,
    `refdrug`.`orgid`,
    `refdrug`.`sortorder`,
    `refdrug`.`status`,
    `refdrug`.`updatedate`,
    `refdrug`.`updateuser`

FROM
    {{ source("bay_dbo", "refdrug") }} AS `refdrug`
