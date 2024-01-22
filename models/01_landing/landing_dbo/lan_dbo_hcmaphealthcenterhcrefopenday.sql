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
            "afternoonshiftendtime",
            "afternoonshiftstarttime",
            "createdate",
            "createuser",
            "hcrefopendayid",
            "healthcenterid",
            "isprovideservice",
            "mappinghealthcenterhcrefopendayid",
            "morningshiftendtime",
            "morningshiftstarttime",
            "orgid",
            "status",
            "updatedate",
            "updateuser"
        ])
    }} AS ingestion_sk,
    afternoonshiftendtime,
    afternoonshiftstarttime,
    createdate,
    createuser,
    hcrefopendayid,
    healthcenterid,
    isprovideservice,
    mappinghealthcenterhcrefopendayid,
    morningshiftendtime,
    morningshiftstarttime,
    orgid,
    status,
    updatedate,
    updateuser

FROM
    {{ source("bay_dbo", "hcmaphealthcenterhcrefopenday") }}
