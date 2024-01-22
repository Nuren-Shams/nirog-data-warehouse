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
            "createdate",
            "createuser",
            "hcmaphealthcenterhcrefoutpatientillnessserviceid",
            "hcrefoutpatientillnessserviceid",
            "healthcenterid",
            "isprovideoutpatientservice",
            "orgid",
            "outpatientillnessservicefee",
            "status",
            "updatedate",
            "updateuser"
        ])
    }} AS ingestion_sk,
    createdate,
    createuser,
    hcmaphealthcenterhcrefoutpatientillnessserviceid,
    hcrefoutpatientillnessserviceid,
    healthcenterid,
    isprovideoutpatientservice,
    orgid,
    outpatientillnessservicefee,
    status,
    updatedate,
    updateuser

FROM
    {{ source("bay_dbo", "hcmaphealthcenterhcrefoutpatientillnessservice") }}
