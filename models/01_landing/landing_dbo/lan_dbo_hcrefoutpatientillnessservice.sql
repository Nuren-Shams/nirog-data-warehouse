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
            "hcrefoutpatientillnessservice.createdate",
            "hcrefoutpatientillnessservice.createuser",
            "hcrefoutpatientillnessservice.hcrefoutpatientillnessserviceid",
            "hcrefoutpatientillnessservice.hcrefoutpatientillnessservicename",
            "hcrefoutpatientillnessservice.orgid",
            "hcrefoutpatientillnessservice.sortorder",
            "hcrefoutpatientillnessservice.status",
            "hcrefoutpatientillnessservice.updatedate",
            "hcrefoutpatientillnessservice.updateuser"
        ])
    }} AS ingestion_sk,
    hcrefoutpatientillnessservice.createdate,
    hcrefoutpatientillnessservice.createuser,
    hcrefoutpatientillnessservice.hcrefoutpatientillnessserviceid,
    hcrefoutpatientillnessservice.hcrefoutpatientillnessservicename,
    hcrefoutpatientillnessservice.orgid,
    hcrefoutpatientillnessservice.sortorder,
    hcrefoutpatientillnessservice.status,
    hcrefoutpatientillnessservice.updatedate,
    hcrefoutpatientillnessservice.updateuser

FROM
    {{ source("bay_dbo", "hcrefoutpatientillnessservice") }} AS hcrefoutpatientillnessservice
