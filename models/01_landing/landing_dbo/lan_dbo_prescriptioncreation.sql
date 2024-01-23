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
            "`prescriptioncreation`.`createdate`",
            "`prescriptioncreation`.`createuser`",
            "`prescriptioncreation`.`employeeid`",
            "`prescriptioncreation`.`orgid`",
            "`prescriptioncreation`.`patientid`",
            "`prescriptioncreation`.`prescriptioncreationid`",
            "`prescriptioncreation`.`prescriptionid`",
            "`prescriptioncreation`.`status`",
            "`prescriptioncreation`.`updatedate`",
            "`prescriptioncreation`.`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `prescriptioncreation`.`createdate`,
    `prescriptioncreation`.`createuser`,
    `prescriptioncreation`.`employeeid`,
    `prescriptioncreation`.`orgid`,
    `prescriptioncreation`.`patientid`,
    `prescriptioncreation`.`prescriptioncreationid`,
    `prescriptioncreation`.`prescriptionid`,
    `prescriptioncreation`.`status`,
    `prescriptioncreation`.`updatedate`,
    `prescriptioncreation`.`updateuser`

FROM
    {{ source("bay_dbo", "prescriptioncreation") }} AS `prescriptioncreation`
