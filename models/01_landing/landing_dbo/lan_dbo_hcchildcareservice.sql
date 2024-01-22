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
            "childcareservice_0_1_fee",
            "childcareservice_1_5_fee",
            "createdate",
            "createuser",
            "hcchildcareserviceid",
            "healthcenterid",
            "isprovidechildcareservice_0_1",
            "isprovidechildcareservice_1_5",
            "orgid",
            "status",
            "updatedate",
            "updateuser"
        ])
    }} AS ingestion_sk,
    childcareservice_0_1_fee,
    childcareservice_1_5_fee,
    createdate,
    createuser,
    hcchildcareserviceid,
    healthcenterid,
    isprovidechildcareservice_0_1,
    isprovidechildcareservice_1_5,
    orgid,
    status,
    updatedate,
    updateuser

FROM
    {{ source("bay_dbo", "hcchildcareservice") }}
