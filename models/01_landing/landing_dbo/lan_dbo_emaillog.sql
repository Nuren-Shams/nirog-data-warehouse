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
            "emaillog.attachedfilenames",
            "emaillog.bccaddress",
            "emaillog.ccaddress",
            "emaillog.createdate",
            "emaillog.createuser",
            "emaillog.emailbody",
            "emaillog.emaileventid",
            "emaillog.emailid",
            "emaillog.emailresponse",
            "emaillog.emailsenddate",
            "emaillog.emailsubject",
            "emaillog.fromaddress",
            "emaillog.isattachedfile",
            "emaillog.issentemail",
            "emaillog.isuploademail",
            "emaillog.orgid",
            "emaillog.patientid",
            "emaillog.status",
            "emaillog.toaddress",
            "emaillog.updatedate",
            "emaillog.updateuser",
            "emaillog.workplacebranchid",
            "emaillog.workplaceid",
            "emaillog.workplacescheduleid"
        ])
    }} AS ingestion_sk,
    emaillog.attachedfilenames,
    emaillog.bccaddress,
    emaillog.ccaddress,
    emaillog.createdate,
    emaillog.createuser,
    emaillog.emailbody,
    emaillog.emaileventid,
    emaillog.emailid,
    emaillog.emailresponse,
    emaillog.emailsenddate,
    emaillog.emailsubject,
    emaillog.fromaddress,
    emaillog.isattachedfile,
    emaillog.issentemail,
    emaillog.isuploademail,
    emaillog.orgid,
    emaillog.patientid,
    emaillog.status,
    emaillog.toaddress,
    emaillog.updatedate,
    emaillog.updateuser,
    emaillog.workplacebranchid,
    emaillog.workplaceid,
    emaillog.workplacescheduleid

FROM
    {{ source("bay_dbo", "emaillog") }} AS emaillog
