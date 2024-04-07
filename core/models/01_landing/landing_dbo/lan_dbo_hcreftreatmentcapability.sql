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
            "`hcreftreatmentcapability`.`createdate`",
            "`hcreftreatmentcapability`.`createuser`",
            "`hcreftreatmentcapability`.`hcreftreatmentcapabilityid`",
            "`hcreftreatmentcapability`.`hctreatmentcapabilityname`",
            "`hcreftreatmentcapability`.`orgid`",
            "`hcreftreatmentcapability`.`sortorder`",
            "`hcreftreatmentcapability`.`status`",
            "`hcreftreatmentcapability`.`updatedate`",
            "`hcreftreatmentcapability`.`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `hcreftreatmentcapability`.`createdate`,
    `hcreftreatmentcapability`.`createuser`,
    `hcreftreatmentcapability`.`hcreftreatmentcapabilityid`,
    `hcreftreatmentcapability`.`hctreatmentcapabilityname`,
    `hcreftreatmentcapability`.`orgid`,
    `hcreftreatmentcapability`.`sortorder`,
    `hcreftreatmentcapability`.`status`,
    `hcreftreatmentcapability`.`updatedate`,
    `hcreftreatmentcapability`.`updateuser`

FROM
    {{ source("bay_dbo", "hcreftreatmentcapability") }} AS `hcreftreatmentcapability`
