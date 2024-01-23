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
            "workplaceschedule.comment",
            "workplaceschedule.createdate",
            "workplaceschedule.createuser",
            "workplaceschedule.isfollowupdate",
            "workplaceschedule.issendemail",
            "workplaceschedule.issendsms",
            "workplaceschedule.orgid",
            "workplaceschedule.scheduleenddate",
            "workplaceschedule.schedulestartdate",
            "workplaceschedule.sendcount",
            "workplaceschedule.senddate",
            "workplaceschedule.sortorder",
            "workplaceschedule.status",
            "workplaceschedule.updatedate",
            "workplaceschedule.updateuser",
            "workplaceschedule.workplacebranchid",
            "workplaceschedule.workplaceid",
            "workplaceschedule.workplacescheduleid"
        ])
    }} AS ingestion_sk,
    workplaceschedule.comment,
    workplaceschedule.createdate,
    workplaceschedule.createuser,
    workplaceschedule.isfollowupdate,
    workplaceschedule.issendemail,
    workplaceschedule.issendsms,
    workplaceschedule.orgid,
    workplaceschedule.scheduleenddate,
    workplaceschedule.schedulestartdate,
    workplaceschedule.sendcount,
    workplaceschedule.senddate,
    workplaceschedule.sortorder,
    workplaceschedule.status,
    workplaceschedule.updatedate,
    workplaceschedule.updateuser,
    workplaceschedule.workplacebranchid,
    workplaceschedule.workplaceid,
    workplaceschedule.workplacescheduleid

FROM
    {{ source("bay_dbo", "workplaceschedule") }} AS workplaceschedule
