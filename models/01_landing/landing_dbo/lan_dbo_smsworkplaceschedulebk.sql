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
            "smsworkplaceschedulebk.createdate",
            "smsworkplaceschedulebk.isfollowupdate",
            "smsworkplaceschedulebk.ispulled",
            "smsworkplaceschedulebk.ispulledforfollowup",
            "smsworkplaceschedulebk.issendsms",
            "smsworkplaceschedulebk.orgid",
            "smsworkplaceschedulebk.scheduleenddate",
            "smsworkplaceschedulebk.schedulestartdate",
            "smsworkplaceschedulebk.smsworkplaceschedulebkid",
            "smsworkplaceschedulebk.smsworkplacescheduleid",
            "smsworkplaceschedulebk.status",
            "smsworkplaceschedulebk.workplacebranchid",
            "smsworkplaceschedulebk.workplaceid",
            "smsworkplaceschedulebk.workplacescheduleid"
        ])
    }} AS ingestion_sk,
    smsworkplaceschedulebk.createdate,
    smsworkplaceschedulebk.isfollowupdate,
    smsworkplaceschedulebk.ispulled,
    smsworkplaceschedulebk.ispulledforfollowup,
    smsworkplaceschedulebk.issendsms,
    smsworkplaceschedulebk.orgid,
    smsworkplaceschedulebk.scheduleenddate,
    smsworkplaceschedulebk.schedulestartdate,
    smsworkplaceschedulebk.smsworkplaceschedulebkid,
    smsworkplaceschedulebk.smsworkplacescheduleid,
    smsworkplaceschedulebk.status,
    smsworkplaceschedulebk.workplacebranchid,
    smsworkplaceschedulebk.workplaceid,
    smsworkplaceschedulebk.workplacescheduleid

FROM
    {{ source("bay_dbo", "smsworkplaceschedulebk") }} AS smsworkplaceschedulebk
