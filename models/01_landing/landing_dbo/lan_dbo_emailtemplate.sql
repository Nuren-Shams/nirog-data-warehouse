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
            "emailtemplate.createdate",
            "emailtemplate.createuser",
            "emailtemplate.emailtemplatebody",
            "emailtemplate.emailtemplateid",
            "emailtemplate.emailtemplatename",
            "emailtemplate.emailtemplatesubject",
            "emailtemplate.orgid",
            "emailtemplate.status",
            "emailtemplate.updatedate",
            "emailtemplate.updateuser"
        ])
    }} AS ingestion_sk,
    emailtemplate.createdate,
    emailtemplate.createuser,
    emailtemplate.emailtemplatebody,
    emailtemplate.emailtemplateid,
    emailtemplate.emailtemplatename,
    emailtemplate.emailtemplatesubject,
    emailtemplate.orgid,
    emailtemplate.status,
    emailtemplate.updatedate,
    emailtemplate.updateuser

FROM
    {{ source("bay_dbo", "emailtemplate") }} AS emailtemplate
