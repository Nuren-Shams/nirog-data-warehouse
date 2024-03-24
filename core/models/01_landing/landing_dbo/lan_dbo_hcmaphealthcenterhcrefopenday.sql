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
            "`hcmaphealthcenterhcrefopenday`.`afternoonshiftendtime`",
            "`hcmaphealthcenterhcrefopenday`.`afternoonshiftstarttime`",
            "`hcmaphealthcenterhcrefopenday`.`createdate`",
            "`hcmaphealthcenterhcrefopenday`.`createuser`",
            "`hcmaphealthcenterhcrefopenday`.`hcrefopendayid`",
            "`hcmaphealthcenterhcrefopenday`.`healthcenterid`",
            "`hcmaphealthcenterhcrefopenday`.`isprovideservice`",
            "`hcmaphealthcenterhcrefopenday`.`mappinghealthcenterhcrefopendayid`",
            "`hcmaphealthcenterhcrefopenday`.`morningshiftendtime`",
            "`hcmaphealthcenterhcrefopenday`.`morningshiftstarttime`",
            "`hcmaphealthcenterhcrefopenday`.`orgid`",
            "`hcmaphealthcenterhcrefopenday`.`status`",
            "`hcmaphealthcenterhcrefopenday`.`updatedate`",
            "`hcmaphealthcenterhcrefopenday`.`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `hcmaphealthcenterhcrefopenday`.`afternoonshiftendtime`,
    `hcmaphealthcenterhcrefopenday`.`afternoonshiftstarttime`,
    `hcmaphealthcenterhcrefopenday`.`createdate`,
    `hcmaphealthcenterhcrefopenday`.`createuser`,
    `hcmaphealthcenterhcrefopenday`.`hcrefopendayid`,
    `hcmaphealthcenterhcrefopenday`.`healthcenterid`,
    `hcmaphealthcenterhcrefopenday`.`isprovideservice`,
    `hcmaphealthcenterhcrefopenday`.`mappinghealthcenterhcrefopendayid`,
    `hcmaphealthcenterhcrefopenday`.`morningshiftendtime`,
    `hcmaphealthcenterhcrefopenday`.`morningshiftstarttime`,
    `hcmaphealthcenterhcrefopenday`.`orgid`,
    `hcmaphealthcenterhcrefopenday`.`status`,
    `hcmaphealthcenterhcrefopenday`.`updatedate`,
    `hcmaphealthcenterhcrefopenday`.`updateuser`

FROM
    {{ source("bay_dbo", "hcmaphealthcenterhcrefopenday") }} AS `hcmaphealthcenterhcrefopenday`
