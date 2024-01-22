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
            "hcreftreatmentcapabilityid",
            "hctreatmentcapabilityfee",
            "healthcenterid",
            "isprovideservice",
            "mappinghealthcenterhcreftreatmentcapabilityid",
            "orgid",
            "status",
            "updatedate",
            "updateuser"
        ])
    }} AS ingestion_sk,
    createdate,
    createuser,
    hcreftreatmentcapabilityid,
    hctreatmentcapabilityfee,
    healthcenterid,
    isprovideservice,
    mappinghealthcenterhcreftreatmentcapabilityid,
    orgid,
    status,
    updatedate,
    updateuser

FROM
    {{ source("bay_dbo", "hcmaphealthcenterhcreftreatmentcapability") }}
