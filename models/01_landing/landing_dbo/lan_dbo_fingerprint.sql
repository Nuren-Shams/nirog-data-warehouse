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
            "fingerprint.createdate",
            "fingerprint.createuser",
            "fingerprint.fingerimage",
            "fingerprint.fingerprintid",
            "fingerprint.fingerprinttype",
            "fingerprint.orgid",
            "fingerprint.status",
            "fingerprint.updatedate",
            "fingerprint.updateuser"
        ])
    }} AS ingestion_sk,
    fingerprint.createdate,
    fingerprint.createuser,
    fingerprint.fingerimage,
    fingerprint.fingerprintid,
    fingerprint.fingerprinttype,
    fingerprint.orgid,
    fingerprint.status,
    fingerprint.updatedate,
    fingerprint.updateuser

FROM
    {{ source("bay_dbo", "fingerprint") }} AS fingerprint
