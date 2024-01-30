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
            "`hcemergencyservice`.`createdate`",
            "`hcemergencyservice`.`createuser`",
            "`hcemergencyservice`.`emergencyservicehoursendtime`",
            "`hcemergencyservice`.`emergencyservicehoursstarttime`",
            "`hcemergencyservice`.`generalemergencyfee`",
            "`hcemergencyservice`.`hcemergencyserviceid`",
            "`hcemergencyservice`.`healthcenterid`",
            "`hcemergencyservice`.`isprovideemergencyservice`",
            "`hcemergencyservice`.`orgid`",
            "`hcemergencyservice`.`status`",
            "`hcemergencyservice`.`updatedate`",
            "`hcemergencyservice`.`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `hcemergencyservice`.`createdate`,
    `hcemergencyservice`.`createuser`,
    `hcemergencyservice`.`emergencyservicehoursendtime`,
    `hcemergencyservice`.`emergencyservicehoursstarttime`,
    `hcemergencyservice`.`generalemergencyfee`,
    `hcemergencyservice`.`hcemergencyserviceid`,
    `hcemergencyservice`.`healthcenterid`,
    `hcemergencyservice`.`isprovideemergencyservice`,
    `hcemergencyservice`.`orgid`,
    `hcemergencyservice`.`status`,
    `hcemergencyservice`.`updatedate`,
    `hcemergencyservice`.`updateuser`

FROM
    {{ source("bay_dbo", "hcemergencyservice") }} AS `hcemergencyservice`
