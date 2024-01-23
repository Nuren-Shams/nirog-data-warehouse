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
            "dsyncdownloaduploadstatus.createdate",
            "dsyncdownloaduploadstatus.createuser",
            "dsyncdownloaduploadstatus.downloaduploadindicator",
            "dsyncdownloaduploadstatus.dsyncdownloaduploadstatusid",
            "dsyncdownloaduploadstatus.ipaddress",
            "dsyncdownloaduploadstatus.isfinalupload",
            "dsyncdownloaduploadstatus.operationdate",
            "dsyncdownloaduploadstatus.updatedate",
            "dsyncdownloaduploadstatus.updateuser",
            "dsyncdownloaduploadstatus.workplaceid",
            "dsyncdownloaduploadstatus.workplacescheduleid"
        ])
    }} AS ingestion_sk,
    dsyncdownloaduploadstatus.createdate,
    dsyncdownloaduploadstatus.createuser,
    dsyncdownloaduploadstatus.downloaduploadindicator,
    dsyncdownloaduploadstatus.dsyncdownloaduploadstatusid,
    dsyncdownloaduploadstatus.ipaddress,
    dsyncdownloaduploadstatus.isfinalupload,
    dsyncdownloaduploadstatus.operationdate,
    dsyncdownloaduploadstatus.updatedate,
    dsyncdownloaduploadstatus.updateuser,
    dsyncdownloaduploadstatus.workplaceid,
    dsyncdownloaduploadstatus.workplacescheduleid

FROM
    {{ source("bay_dbo", "dsyncdownloaduploadstatus") }} AS dsyncdownloaduploadstatus
