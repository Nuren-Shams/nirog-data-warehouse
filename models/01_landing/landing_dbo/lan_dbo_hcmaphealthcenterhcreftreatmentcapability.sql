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
            "`hcmaphealthcenterhcreftreatmentcapability`.`createdate`",
            "`hcmaphealthcenterhcreftreatmentcapability`.`createuser`",
            "`hcmaphealthcenterhcreftreatmentcapability`.`hcreftreatmentcapabilityid`",
            "`hcmaphealthcenterhcreftreatmentcapability`.`hctreatmentcapabilityfee`",
            "`hcmaphealthcenterhcreftreatmentcapability`.`healthcenterid`",
            "`hcmaphealthcenterhcreftreatmentcapability`.`isprovideservice`",
            "`hcmaphealthcenterhcreftreatmentcapability`.`mappinghealthcenterhcreftreatmentcapabilityid`",
            "`hcmaphealthcenterhcreftreatmentcapability`.`orgid`",
            "`hcmaphealthcenterhcreftreatmentcapability`.`status`",
            "`hcmaphealthcenterhcreftreatmentcapability`.`updatedate`",
            "`hcmaphealthcenterhcreftreatmentcapability`.`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `hcmaphealthcenterhcreftreatmentcapability`.`createdate`,
    `hcmaphealthcenterhcreftreatmentcapability`.`createuser`,
    `hcmaphealthcenterhcreftreatmentcapability`.`hcreftreatmentcapabilityid`,
    `hcmaphealthcenterhcreftreatmentcapability`.`hctreatmentcapabilityfee`,
    `hcmaphealthcenterhcreftreatmentcapability`.`healthcenterid`,
    `hcmaphealthcenterhcreftreatmentcapability`.`isprovideservice`,
    `hcmaphealthcenterhcreftreatmentcapability`.`mappinghealthcenterhcreftreatmentcapabilityid`,
    `hcmaphealthcenterhcreftreatmentcapability`.`orgid`,
    `hcmaphealthcenterhcreftreatmentcapability`.`status`,
    `hcmaphealthcenterhcreftreatmentcapability`.`updatedate`,
    `hcmaphealthcenterhcreftreatmentcapability`.`updateuser`

FROM
    {{ source("bay_dbo", "hcmaphealthcenterhcreftreatmentcapability") }} AS `hcmaphealthcenterhcreftreatmentcapability`
