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
            "`hcrefopenday`.`createdate`",
            "`hcrefopenday`.`createuser`",
            "`hcrefopenday`.`hcrefopendayid`",
            "`hcrefopenday`.`opendayname`",
            "`hcrefopenday`.`orgid`",
            "`hcrefopenday`.`sortorder`",
            "`hcrefopenday`.`status`",
            "`hcrefopenday`.`updatedate`",
            "`hcrefopenday`.`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `hcrefopenday`.`createdate`,
    `hcrefopenday`.`createuser`,
    `hcrefopenday`.`hcrefopendayid`,
    `hcrefopenday`.`opendayname`,
    `hcrefopenday`.`orgid`,
    `hcrefopenday`.`sortorder`,
    `hcrefopenday`.`status`,
    `hcrefopenday`.`updatedate`,
    `hcrefopenday`.`updateuser`

FROM
    {{ source("bay_dbo", "hcrefopenday") }} AS `hcrefopenday`
