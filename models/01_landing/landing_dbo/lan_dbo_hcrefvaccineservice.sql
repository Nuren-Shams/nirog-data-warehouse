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
            "hcrefvaccineservice.createdate",
            "hcrefvaccineservice.createuser",
            "hcrefvaccineservice.hcrefvaccineserviceid",
            "hcrefvaccineservice.hcrefvaccineservicename",
            "hcrefvaccineservice.orgid",
            "hcrefvaccineservice.sortorder",
            "hcrefvaccineservice.status",
            "hcrefvaccineservice.updatedate",
            "hcrefvaccineservice.updateuser",
            "hcrefvaccineservice.vaccinetype"
        ])
    }} AS ingestion_sk,
    hcrefvaccineservice.createdate,
    hcrefvaccineservice.createuser,
    hcrefvaccineservice.hcrefvaccineserviceid,
    hcrefvaccineservice.hcrefvaccineservicename,
    hcrefvaccineservice.orgid,
    hcrefvaccineservice.sortorder,
    hcrefvaccineservice.status,
    hcrefvaccineservice.updatedate,
    hcrefvaccineservice.updateuser,
    hcrefvaccineservice.vaccinetype

FROM
    {{ source("bay_dbo", "hcrefvaccineservice") }} AS hcrefvaccineservice
