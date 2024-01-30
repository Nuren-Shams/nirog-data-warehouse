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
            "`hcmaphealthcenterhcrx`.`createdate`",
            "`hcmaphealthcenterhcrx`.`createuser`",
            "`hcmaphealthcenterhcrx`.`hcmaphealthcenterhcrxid`",
            "`hcmaphealthcenterhcrx`.`hcrxid`",
            "`hcmaphealthcenterhcrx`.`healthcenterid`",
            "`hcmaphealthcenterhcrx`.`isproviderx`",
            "`hcmaphealthcenterhcrx`.`orgid`",
            "`hcmaphealthcenterhcrx`.`rxfee`",
            "`hcmaphealthcenterhcrx`.`status`",
            "`hcmaphealthcenterhcrx`.`updatedate`",
            "`hcmaphealthcenterhcrx`.`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `hcmaphealthcenterhcrx`.`createdate`,
    `hcmaphealthcenterhcrx`.`createuser`,
    `hcmaphealthcenterhcrx`.`hcmaphealthcenterhcrxid`,
    `hcmaphealthcenterhcrx`.`hcrxid`,
    `hcmaphealthcenterhcrx`.`healthcenterid`,
    `hcmaphealthcenterhcrx`.`isproviderx`,
    `hcmaphealthcenterhcrx`.`orgid`,
    `hcmaphealthcenterhcrx`.`rxfee`,
    `hcmaphealthcenterhcrx`.`status`,
    `hcmaphealthcenterhcrx`.`updatedate`,
    `hcmaphealthcenterhcrx`.`updateuser`

FROM
    {{ source("bay_dbo", "hcmaphealthcenterhcrx") }} AS `hcmaphealthcenterhcrx`
