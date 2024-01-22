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
            "hcrefvaccineserviceid",
            "hcrefvaccineservicename",
            "orgid",
            "sortorder",
            "status",
            "updatedate",
            "updateuser",
            "vaccinetype"
        ])
    }} AS ingestion_sk,
    createdate,
    createuser,
    hcrefvaccineserviceid,
    hcrefvaccineservicename,
    orgid,
    sortorder,
    status,
    updatedate,
    updateuser,
    vaccinetype

FROM
    {{ source("bay_dbo", "hcrefvaccineservice") }}
