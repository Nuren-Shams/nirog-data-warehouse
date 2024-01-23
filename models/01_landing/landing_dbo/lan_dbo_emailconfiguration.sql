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
            "emailconfiguration.configid",
            "emailconfiguration.configkey",
            "emailconfiguration.configvalue",
            "emailconfiguration.createdate",
            "emailconfiguration.createuser",
            "emailconfiguration.description",
            "emailconfiguration.orgid",
            "emailconfiguration.status",
            "emailconfiguration.updatedate",
            "emailconfiguration.updateuser"
        ])
    }} AS ingestion_sk,
    emailconfiguration.configid,
    emailconfiguration.configkey,
    emailconfiguration.configvalue,
    emailconfiguration.createdate,
    emailconfiguration.createuser,
    emailconfiguration.description,
    emailconfiguration.orgid,
    emailconfiguration.status,
    emailconfiguration.updatedate,
    emailconfiguration.updateuser

FROM
    {{ source("bay_dbo", "emailconfiguration") }} AS emailconfiguration
