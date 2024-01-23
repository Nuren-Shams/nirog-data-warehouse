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
            "hcchildcareservice.childcareservice_0_1_fee",
            "hcchildcareservice.childcareservice_1_5_fee",
            "hcchildcareservice.createdate",
            "hcchildcareservice.createuser",
            "hcchildcareservice.hcchildcareserviceid",
            "hcchildcareservice.healthcenterid",
            "hcchildcareservice.isprovidechildcareservice_0_1",
            "hcchildcareservice.isprovidechildcareservice_1_5",
            "hcchildcareservice.orgid",
            "hcchildcareservice.status",
            "hcchildcareservice.updatedate",
            "hcchildcareservice.updateuser"
        ])
    }} AS ingestion_sk,
    hcchildcareservice.childcareservice_0_1_fee,
    hcchildcareservice.childcareservice_1_5_fee,
    hcchildcareservice.createdate,
    hcchildcareservice.createuser,
    hcchildcareservice.hcchildcareserviceid,
    hcchildcareservice.healthcenterid,
    hcchildcareservice.isprovidechildcareservice_0_1,
    hcchildcareservice.isprovidechildcareservice_1_5,
    hcchildcareservice.orgid,
    hcchildcareservice.status,
    hcchildcareservice.updatedate,
    hcchildcareservice.updateuser

FROM
    {{ source("bay_dbo", "hcchildcareservice") }} AS hcchildcareservice
