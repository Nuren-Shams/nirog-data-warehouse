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
            "mdatabp.bpdiastolic1",
            "mdatabp.bpdiastolic2",
            "mdatabp.bpsystolic1",
            "mdatabp.bpsystolic2",
            "mdatabp.collectiondate",
            "mdatabp.createdate",
            "mdatabp.createuser",
            "mdatabp.currenttemparature",
            "mdatabp.heartrate",
            "mdatabp.id",
            "mdatabp.indicatesnormaloxygensaturation",
            "mdatabp.orgid",
            "mdatabp.patientid",
            "mdatabp.respiratoryrate",
            "mdatabp.spo2rate",
            "mdatabp.status",
            "mdatabp.updatedate",
            "mdatabp.updateuser"
        ])
    }} AS ingestion_sk,
    mdatabp.bpdiastolic1,
    mdatabp.bpdiastolic2,
    mdatabp.bpsystolic1,
    mdatabp.bpsystolic2,
    mdatabp.collectiondate,
    mdatabp.createdate,
    mdatabp.createuser,
    mdatabp.currenttemparature,
    mdatabp.heartrate,
    mdatabp.id,
    mdatabp.indicatesnormaloxygensaturation,
    mdatabp.orgid,
    mdatabp.patientid,
    mdatabp.respiratoryrate,
    mdatabp.spo2rate,
    mdatabp.status,
    mdatabp.updatedate,
    mdatabp.updateuser

FROM
    {{ source("bay_dbo", "mdatabp") }} AS mdatabp
