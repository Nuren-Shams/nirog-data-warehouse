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
            "hcmaphealthcenterhcrefoutpatientillnessservice.createdate",
            "hcmaphealthcenterhcrefoutpatientillnessservice.createuser",
            "hcmaphealthcenterhcrefoutpatientillnessservice.hcmaphealthcenterhcrefoutpatientillnessserviceid",
            "hcmaphealthcenterhcrefoutpatientillnessservice.hcrefoutpatientillnessserviceid",
            "hcmaphealthcenterhcrefoutpatientillnessservice.healthcenterid",
            "hcmaphealthcenterhcrefoutpatientillnessservice.isprovideoutpatientservice",
            "hcmaphealthcenterhcrefoutpatientillnessservice.orgid",
            "hcmaphealthcenterhcrefoutpatientillnessservice.outpatientillnessservicefee",
            "hcmaphealthcenterhcrefoutpatientillnessservice.status",
            "hcmaphealthcenterhcrefoutpatientillnessservice.updatedate",
            "hcmaphealthcenterhcrefoutpatientillnessservice.updateuser"
        ])
    }} AS ingestion_sk,
    hcmaphealthcenterhcrefoutpatientillnessservice.createdate,
    hcmaphealthcenterhcrefoutpatientillnessservice.createuser,
    hcmaphealthcenterhcrefoutpatientillnessservice.hcmaphealthcenterhcrefoutpatientillnessserviceid,
    hcmaphealthcenterhcrefoutpatientillnessservice.hcrefoutpatientillnessserviceid,
    hcmaphealthcenterhcrefoutpatientillnessservice.healthcenterid,
    hcmaphealthcenterhcrefoutpatientillnessservice.isprovideoutpatientservice,
    hcmaphealthcenterhcrefoutpatientillnessservice.orgid,
    hcmaphealthcenterhcrefoutpatientillnessservice.outpatientillnessservicefee,
    hcmaphealthcenterhcrefoutpatientillnessservice.status,
    hcmaphealthcenterhcrefoutpatientillnessservice.updatedate,
    hcmaphealthcenterhcrefoutpatientillnessservice.updateuser

FROM
    {{ source("bay_dbo", "hcmaphealthcenterhcrefoutpatientillnessservice") }} AS hcmaphealthcenterhcrefoutpatientillnessservice
