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
            "eclampsiatreatmentfee",
            "hcpostnatalcareid",
            "healthcenterid",
            "isprovideeclampsiatreatment",
            "isprovidenalepsintreatment",
            "isprovidepphtreatment",
            "isprovidepreeclampsiatreatment",
            "nalepsintreatmentfee",
            "orgid",
            "pphtreatmentfee",
            "preeclampsiatreatmentfee",
            "status",
            "updatedate",
            "updateuser"
        ])
    }} AS ingestion_sk,
    createdate,
    createuser,
    eclampsiatreatmentfee,
    hcpostnatalcareid,
    healthcenterid,
    isprovideeclampsiatreatment,
    isprovidenalepsintreatment,
    isprovidepphtreatment,
    isprovidepreeclampsiatreatment,
    nalepsintreatmentfee,
    orgid,
    pphtreatmentfee,
    preeclampsiatreatmentfee,
    status,
    updatedate,
    updateuser

FROM
    {{ source("bay_dbo", "hcpostnatalcare") }}
