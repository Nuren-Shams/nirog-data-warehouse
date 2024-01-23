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
            "hcpostnatalcare.createdate",
            "hcpostnatalcare.createuser",
            "hcpostnatalcare.eclampsiatreatmentfee",
            "hcpostnatalcare.hcpostnatalcareid",
            "hcpostnatalcare.healthcenterid",
            "hcpostnatalcare.isprovideeclampsiatreatment",
            "hcpostnatalcare.isprovidenalepsintreatment",
            "hcpostnatalcare.isprovidepphtreatment",
            "hcpostnatalcare.isprovidepreeclampsiatreatment",
            "hcpostnatalcare.nalepsintreatmentfee",
            "hcpostnatalcare.orgid",
            "hcpostnatalcare.pphtreatmentfee",
            "hcpostnatalcare.preeclampsiatreatmentfee",
            "hcpostnatalcare.status",
            "hcpostnatalcare.updatedate",
            "hcpostnatalcare.updateuser"
        ])
    }} AS ingestion_sk,
    hcpostnatalcare.createdate,
    hcpostnatalcare.createuser,
    hcpostnatalcare.eclampsiatreatmentfee,
    hcpostnatalcare.hcpostnatalcareid,
    hcpostnatalcare.healthcenterid,
    hcpostnatalcare.isprovideeclampsiatreatment,
    hcpostnatalcare.isprovidenalepsintreatment,
    hcpostnatalcare.isprovidepphtreatment,
    hcpostnatalcare.isprovidepreeclampsiatreatment,
    hcpostnatalcare.nalepsintreatmentfee,
    hcpostnatalcare.orgid,
    hcpostnatalcare.pphtreatmentfee,
    hcpostnatalcare.preeclampsiatreatmentfee,
    hcpostnatalcare.status,
    hcpostnatalcare.updatedate,
    hcpostnatalcare.updateuser

FROM
    {{ source("bay_dbo", "hcpostnatalcare") }} AS hcpostnatalcare
