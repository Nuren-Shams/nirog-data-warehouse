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
            "mdataheightweight.bmi",
            "mdataheightweight.bmistatus",
            "mdataheightweight.collectiondate",
            "mdataheightweight.createdate",
            "mdataheightweight.createuser",
            "mdataheightweight.height",
            "mdataheightweight.id",
            "mdataheightweight.muac",
            "mdataheightweight.muacstatus",
            "mdataheightweight.orgid",
            "mdataheightweight.patientid",
            "mdataheightweight.refbloodgroupid",
            "mdataheightweight.status",
            "mdataheightweight.updatedate",
            "mdataheightweight.updateuser",
            "mdataheightweight.weight"
        ])
    }} AS ingestion_sk,
    mdataheightweight.bmi,
    mdataheightweight.bmistatus,
    mdataheightweight.collectiondate,
    mdataheightweight.createdate,
    mdataheightweight.createuser,
    mdataheightweight.height,
    mdataheightweight.id,
    mdataheightweight.muac,
    mdataheightweight.muacstatus,
    mdataheightweight.orgid,
    mdataheightweight.patientid,
    mdataheightweight.refbloodgroupid,
    mdataheightweight.status,
    mdataheightweight.updatedate,
    mdataheightweight.updateuser,
    mdataheightweight.weight

FROM
    {{ source("bay_dbo", "mdataheightweight") }} AS mdataheightweight
