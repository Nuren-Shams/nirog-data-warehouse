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
            "refbiopsyresult.biopsyresultcode",
            "refbiopsyresult.biopsyresultid",
            "refbiopsyresult.createdate",
            "refbiopsyresult.createuser",
            "refbiopsyresult.description",
            "refbiopsyresult.orgid",
            "refbiopsyresult.sortorder",
            "refbiopsyresult.status",
            "refbiopsyresult.updatedate",
            "refbiopsyresult.updateuser"
        ])
    }} AS ingestion_sk,
    refbiopsyresult.biopsyresultcode,
    refbiopsyresult.biopsyresultid,
    refbiopsyresult.createdate,
    refbiopsyresult.createuser,
    refbiopsyresult.description,
    refbiopsyresult.orgid,
    refbiopsyresult.sortorder,
    refbiopsyresult.status,
    refbiopsyresult.updatedate,
    refbiopsyresult.updateuser

FROM
    {{ source("bay_dbo", "refbiopsyresult") }} AS refbiopsyresult
