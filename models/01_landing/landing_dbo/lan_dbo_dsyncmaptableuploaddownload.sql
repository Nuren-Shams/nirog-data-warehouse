{{
    config(
        materialized = "incremental",
        unique_key = "ingestion_sk",
        tags = ["execute_daily"]
    )
}}


SELECT
    {{
        dbt_utils.generate_surrogate_key([
            "download_flag",
            "lock_ind",
            "orgid_flag",
            "patientid_ind",
            "tableid",
            "tablename",
            "upload_flag",
            "workplaceid_ind"
        ])
    }} AS ingestion_sk,
    download_flag,
    lock_ind,
    orgid_flag,
    patientid_ind,
    tableid,
    tablename,
    upload_flag,
    workplaceid_ind

FROM
    {{ source("bay_dbo", "dsyncmaptableuploaddownload") }}
