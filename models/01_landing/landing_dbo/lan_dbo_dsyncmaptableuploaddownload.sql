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
            "`dsyncmaptableuploaddownload`.`download_flag`",
            "`dsyncmaptableuploaddownload`.`lock_ind`",
            "`dsyncmaptableuploaddownload`.`orgid_flag`",
            "`dsyncmaptableuploaddownload`.`patientid_ind`",
            "`dsyncmaptableuploaddownload`.`tableid`",
            "`dsyncmaptableuploaddownload`.`tablename`",
            "`dsyncmaptableuploaddownload`.`upload_flag`",
            "`dsyncmaptableuploaddownload`.`workplaceid_ind`"
        ])
    }} AS `ingestion_sk`,
    `dsyncmaptableuploaddownload`.`download_flag`,
    `dsyncmaptableuploaddownload`.`lock_ind`,
    `dsyncmaptableuploaddownload`.`orgid_flag`,
    `dsyncmaptableuploaddownload`.`patientid_ind`,
    `dsyncmaptableuploaddownload`.`tableid`,
    `dsyncmaptableuploaddownload`.`tablename`,
    `dsyncmaptableuploaddownload`.`upload_flag`,
    `dsyncmaptableuploaddownload`.`workplaceid_ind`

FROM
    {{ source("bay_dbo", "dsyncmaptableuploaddownload") }} AS `dsyncmaptableuploaddownload`
