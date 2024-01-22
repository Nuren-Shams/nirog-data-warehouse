{{
    config(
        materialized = "incremental",
        unique_key = "ingestion_sk",
        tags = ["execute_daily"]
    )
}}


SELECT
    {{
        dbt_utils.generate_surrogate_key([
            "biopsyresultid",
            "bpdiastolic",
            "bpsystolic",
            "cancertreatmentid",
            "collectiondate",
            "createdate",
            "createuser",
            "doctorcomment",
            "doctorname",
            "fbg",
            "healthcenterid",
            "mdatapatientreferralbackid",
            "orgid",
            "otherfindings",
            "otherpalliativecare",
            "patientid",
            "presenthealthstatusid",
            "rbg",
            "rid",
            "status",
            "tbsputumteststatusid",
            "updatedate",
            "updateuser"
        ])
    }} AS ingestion_sk,
    biopsyresultid,
    bpdiastolic,
    bpsystolic,
    cancertreatmentid,
    collectiondate,
    createdate,
    createuser,
    doctorcomment,
    doctorname,
    fbg,
    healthcenterid,
    mdatapatientreferralbackid,
    orgid,
    otherfindings,
    otherpalliativecare,
    patientid,
    presenthealthstatusid,
    rbg,
    rid,
    status,
    tbsputumteststatusid,
    updatedate,
    updateuser

FROM
    {{ source("bay_dbo", "mdatapatientreferralback") }}
