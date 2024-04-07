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
            "`hcrefdiagnostic`.`createdate`",
            "`hcrefdiagnostic`.`createuser`",
            "`hcrefdiagnostic`.`hcdiagnosticname`",
            "`hcrefdiagnostic`.`hcrefdiagnosticid`",
            "`hcrefdiagnostic`.`orgid`",
            "`hcrefdiagnostic`.`sortorder`",
            "`hcrefdiagnostic`.`status`",
            "`hcrefdiagnostic`.`testtype`",
            "`hcrefdiagnostic`.`updatedate`",
            "`hcrefdiagnostic`.`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `hcrefdiagnostic`.`createdate`,
    `hcrefdiagnostic`.`createuser`,
    `hcrefdiagnostic`.`hcdiagnosticname`,
    `hcrefdiagnostic`.`hcrefdiagnosticid`,
    `hcrefdiagnostic`.`orgid`,
    `hcrefdiagnostic`.`sortorder`,
    `hcrefdiagnostic`.`status`,
    `hcrefdiagnostic`.`testtype`,
    `hcrefdiagnostic`.`updatedate`,
    `hcrefdiagnostic`.`updateuser`

FROM
    {{ source("bay_dbo", "hcrefdiagnostic") }} AS `hcrefdiagnostic`
