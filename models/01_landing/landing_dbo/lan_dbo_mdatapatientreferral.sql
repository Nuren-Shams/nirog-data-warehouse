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
            "mdatapatientreferral.collectiondate",
            "mdatapatientreferral.createdate",
            "mdatapatientreferral.createuser",
            "mdatapatientreferral.healthcenterid",
            "mdatapatientreferral.mdpatientreferralid",
            "mdatapatientreferral.orgid",
            "mdatapatientreferral.patientid",
            "mdatapatientreferral.rid",
            "mdatapatientreferral.status",
            "mdatapatientreferral.updatedate",
            "mdatapatientreferral.updateuser"
        ])
    }} AS ingestion_sk,
    mdatapatientreferral.collectiondate,
    mdatapatientreferral.createdate,
    mdatapatientreferral.createuser,
    mdatapatientreferral.healthcenterid,
    mdatapatientreferral.mdpatientreferralid,
    mdatapatientreferral.orgid,
    mdatapatientreferral.patientid,
    mdatapatientreferral.rid,
    mdatapatientreferral.status,
    mdatapatientreferral.updatedate,
    mdatapatientreferral.updateuser

FROM
    {{ source("bay_dbo", "mdatapatientreferral") }} AS mdatapatientreferral
