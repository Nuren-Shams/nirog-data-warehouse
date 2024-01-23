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
            "reftbsymptoms.createdate",
            "reftbsymptoms.createuser",
            "reftbsymptoms.description",
            "reftbsymptoms.orgid",
            "reftbsymptoms.sortorder",
            "reftbsymptoms.status",
            "reftbsymptoms.tbsymptomcode",
            "reftbsymptoms.tbsymptomid",
            "reftbsymptoms.updatedate",
            "reftbsymptoms.updateuser"
        ])
    }} AS ingestion_sk,
    reftbsymptoms.createdate,
    reftbsymptoms.createuser,
    reftbsymptoms.description,
    reftbsymptoms.orgid,
    reftbsymptoms.sortorder,
    reftbsymptoms.status,
    reftbsymptoms.tbsymptomcode,
    reftbsymptoms.tbsymptomid,
    reftbsymptoms.updatedate,
    reftbsymptoms.updateuser

FROM
    {{ source("bay_dbo", "reftbsymptoms") }} AS reftbsymptoms
