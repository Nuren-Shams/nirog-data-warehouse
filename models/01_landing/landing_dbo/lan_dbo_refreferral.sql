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
            "`refreferral`.`createdate`",
            "`refreferral`.`createuser`",
            "`refreferral`.`description`",
            "`refreferral`.`orgid`",
            "`refreferral`.`rcode`",
            "`refreferral`.`rid`",
            "`refreferral`.`sortorder`",
            "`refreferral`.`status`",
            "`refreferral`.`updatedate`",
            "`refreferral`.`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `refreferral`.`createdate`,
    `refreferral`.`createuser`,
    `refreferral`.`description`,
    `refreferral`.`orgid`,
    `refreferral`.`rcode`,
    `refreferral`.`rid`,
    `refreferral`.`sortorder`,
    `refreferral`.`status`,
    `refreferral`.`updatedate`,
    `refreferral`.`updateuser`

FROM
    {{ source("bay_dbo", "refreferral") }} AS `refreferral`
