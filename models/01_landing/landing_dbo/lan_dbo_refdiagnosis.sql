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
            "`refdiagnosis`.`createdate`",
            "`refdiagnosis`.`createuser`",
            "`refdiagnosis`.`description`",
            "`refdiagnosis`.`diagnosiscode`",
            "`refdiagnosis`.`diagnosisid`",
            "`refdiagnosis`.`orgid`",
            "`refdiagnosis`.`sortorder`",
            "`refdiagnosis`.`status`",
            "`refdiagnosis`.`updatedate`",
            "`refdiagnosis`.`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `refdiagnosis`.`createdate`,
    `refdiagnosis`.`createuser`,
    `refdiagnosis`.`description`,
    `refdiagnosis`.`diagnosiscode`,
    `refdiagnosis`.`diagnosisid`,
    `refdiagnosis`.`orgid`,
    `refdiagnosis`.`sortorder`,
    `refdiagnosis`.`status`,
    `refdiagnosis`.`updatedate`,
    `refdiagnosis`.`updateuser`

FROM
    {{ source("bay_dbo", "refdiagnosis") }} AS `refdiagnosis`
