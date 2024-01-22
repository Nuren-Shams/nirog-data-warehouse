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
            "`childrenvaccinefee`",
            "`createdate`",
            "`createuser`",
            "`hcmaphealthcenterhcrefvaccineserviceid`",
            "`hcrefvaccineserviceid`",
            "`healthcenterid`",
            "`isprovidechildrenvaccine`",
            "`orgid`",
            "`status`",
            "`updatedate`",
            "`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `childrenvaccinefee`,
    `createdate`,
    `createuser`,
    `hcmaphealthcenterhcrefvaccineserviceid`,
    `hcrefvaccineserviceid`,
    `healthcenterid`,
    `isprovidechildrenvaccine`,
    `orgid`,
    `status`,
    `updatedate`,
    `updateuser`

FROM
    {{ source("bay_dbo", "hcmaphealthcenterhcrefvaccineservice") }}
