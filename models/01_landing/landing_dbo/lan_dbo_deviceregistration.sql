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
            "deviceregistration.createdate",
            "deviceregistration.createuserid",
            "deviceregistration.deviceid",
            "deviceregistration.devicename",
            "deviceregistration.macid",
            "deviceregistration.status",
            "deviceregistration.updatedate",
            "deviceregistration.updateuserid",
            "deviceregistration.userid",
            "deviceregistration.workplacebranchid"
        ])
    }} AS ingestion_sk,
    deviceregistration.createdate,
    deviceregistration.createuserid,
    deviceregistration.deviceid,
    deviceregistration.devicename,
    deviceregistration.macid,
    deviceregistration.status,
    deviceregistration.updatedate,
    deviceregistration.updateuserid,
    deviceregistration.userid,
    deviceregistration.workplacebranchid

FROM
    {{ source("bay_dbo", "deviceregistration") }} AS deviceregistration
