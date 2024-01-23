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
            "`hcrx`.`createdate`",
            "`hcrx`.`createuser`",
            "`hcrx`.`hcrxid`",
            "`hcrx`.`hcrxname`",
            "`hcrx`.`orgid`",
            "`hcrx`.`rxtype`",
            "`hcrx`.`sortorder`",
            "`hcrx`.`status`",
            "`hcrx`.`updatedate`",
            "`hcrx`.`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `hcrx`.`createdate`,
    `hcrx`.`createuser`,
    `hcrx`.`hcrxid`,
    `hcrx`.`hcrxname`,
    `hcrx`.`orgid`,
    `hcrx`.`rxtype`,
    `hcrx`.`sortorder`,
    `hcrx`.`status`,
    `hcrx`.`updatedate`,
    `hcrx`.`updateuser`

FROM
    {{ source("bay_dbo", "hcrx") }} AS `hcrx`
