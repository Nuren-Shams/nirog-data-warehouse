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
            "emailevent.createdate",
            "emailevent.createuser",
            "emailevent.emaileventid",
            "emailevent.emaileventname",
            "emailevent.emailtemplateid",
            "emailevent.orgid",
            "emailevent.status",
            "emailevent.updatedate",
            "emailevent.updateuser"
        ])
    }} AS ingestion_sk,
    emailevent.createdate,
    emailevent.createuser,
    emailevent.emaileventid,
    emailevent.emaileventname,
    emailevent.emailtemplateid,
    emailevent.orgid,
    emailevent.status,
    emailevent.updatedate,
    emailevent.updateuser

FROM
    {{ source("bay_dbo", "emailevent") }} AS emailevent
