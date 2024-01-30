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
            "`reftbsputumtest`.`createdate`",
            "`reftbsputumtest`.`createuser`",
            "`reftbsputumtest`.`description`",
            "`reftbsputumtest`.`orgid`",
            "`reftbsputumtest`.`sortorder`",
            "`reftbsputumtest`.`status`",
            "`reftbsputumtest`.`tbsputumtestcode`",
            "`reftbsputumtest`.`tbsputumtestid`",
            "`reftbsputumtest`.`updatedate`",
            "`reftbsputumtest`.`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `reftbsputumtest`.`createdate`,
    `reftbsputumtest`.`createuser`,
    `reftbsputumtest`.`description`,
    `reftbsputumtest`.`orgid`,
    `reftbsputumtest`.`sortorder`,
    `reftbsputumtest`.`status`,
    `reftbsputumtest`.`tbsputumtestcode`,
    `reftbsputumtest`.`tbsputumtestid`,
    `reftbsputumtest`.`updatedate`,
    `reftbsputumtest`.`updateuser`

FROM
    {{ source("bay_dbo", "reftbsputumtest") }} AS `reftbsputumtest`
