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
            "emailmapworkplaceschedule.createdate",
            "emailmapworkplaceschedule.createuser",
            "emailmapworkplaceschedule.emaileventid",
            "emailmapworkplaceschedule.emailmapworkplacescheduleid",
            "emailmapworkplaceschedule.emailsenddate",
            "emailmapworkplaceschedule.orgid",
            "emailmapworkplaceschedule.status",
            "emailmapworkplaceschedule.totalsentemail",
            "emailmapworkplaceschedule.totaluploademail",
            "emailmapworkplaceschedule.totalvisited",
            "emailmapworkplaceschedule.updatedate",
            "emailmapworkplaceschedule.updateuser",
            "emailmapworkplaceschedule.workplacebranchid",
            "emailmapworkplaceschedule.workplaceid",
            "emailmapworkplaceschedule.workplacescheduleid"
        ])
    }} AS ingestion_sk,
    emailmapworkplaceschedule.createdate,
    emailmapworkplaceschedule.createuser,
    emailmapworkplaceschedule.emaileventid,
    emailmapworkplaceschedule.emailmapworkplacescheduleid,
    emailmapworkplaceschedule.emailsenddate,
    emailmapworkplaceschedule.orgid,
    emailmapworkplaceschedule.status,
    emailmapworkplaceschedule.totalsentemail,
    emailmapworkplaceschedule.totaluploademail,
    emailmapworkplaceschedule.totalvisited,
    emailmapworkplaceschedule.updatedate,
    emailmapworkplaceschedule.updateuser,
    emailmapworkplaceschedule.workplacebranchid,
    emailmapworkplaceschedule.workplaceid,
    emailmapworkplaceschedule.workplacescheduleid

FROM
    {{ source("bay_dbo", "emailmapworkplaceschedule") }} AS emailmapworkplaceschedule
