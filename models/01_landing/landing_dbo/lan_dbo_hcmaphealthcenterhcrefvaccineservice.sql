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
            "`hcmaphealthcenterhcrefvaccineservice`.`childrenvaccinefee`",
            "`hcmaphealthcenterhcrefvaccineservice`.`createdate`",
            "`hcmaphealthcenterhcrefvaccineservice`.`createuser`",
            "`hcmaphealthcenterhcrefvaccineservice`.`hcmaphealthcenterhcrefvaccineserviceid`",
            "`hcmaphealthcenterhcrefvaccineservice`.`hcrefvaccineserviceid`",
            "`hcmaphealthcenterhcrefvaccineservice`.`healthcenterid`",
            "`hcmaphealthcenterhcrefvaccineservice`.`isprovidechildrenvaccine`",
            "`hcmaphealthcenterhcrefvaccineservice`.`orgid`",
            "`hcmaphealthcenterhcrefvaccineservice`.`status`",
            "`hcmaphealthcenterhcrefvaccineservice`.`updatedate`",
            "`hcmaphealthcenterhcrefvaccineservice`.`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `hcmaphealthcenterhcrefvaccineservice`.`childrenvaccinefee`,
    `hcmaphealthcenterhcrefvaccineservice`.`createdate`,
    `hcmaphealthcenterhcrefvaccineservice`.`createuser`,
    `hcmaphealthcenterhcrefvaccineservice`.`hcmaphealthcenterhcrefvaccineserviceid`,
    `hcmaphealthcenterhcrefvaccineservice`.`hcrefvaccineserviceid`,
    `hcmaphealthcenterhcrefvaccineservice`.`healthcenterid`,
    `hcmaphealthcenterhcrefvaccineservice`.`isprovidechildrenvaccine`,
    `hcmaphealthcenterhcrefvaccineservice`.`orgid`,
    `hcmaphealthcenterhcrefvaccineservice`.`status`,
    `hcmaphealthcenterhcrefvaccineservice`.`updatedate`,
    `hcmaphealthcenterhcrefvaccineservice`.`updateuser`

FROM
    {{ source("bay_dbo", "hcmaphealthcenterhcrefvaccineservice") }} AS `hcmaphealthcenterhcrefvaccineservice`
