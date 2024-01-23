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
            "`refprovisionaldiagnosis`.`createdate`",
            "`refprovisionaldiagnosis`.`createuser`",
            "`refprovisionaldiagnosis`.`description`",
            "`refprovisionaldiagnosis`.`groupsortorder`",
            "`refprovisionaldiagnosis`.`orgid`",
            "`refprovisionaldiagnosis`.`provisionaldiagnosiscode`",
            "`refprovisionaldiagnosis`.`provisionaldiagnosisname`",
            "`refprovisionaldiagnosis`.`refprovisionaldiagnosisgroupid`",
            "`refprovisionaldiagnosis`.`refprovisionaldiagnosisid`",
            "`refprovisionaldiagnosis`.`sortorder`",
            "`refprovisionaldiagnosis`.`status`",
            "`refprovisionaldiagnosis`.`updatedate`",
            "`refprovisionaldiagnosis`.`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `refprovisionaldiagnosis`.`createdate`,
    `refprovisionaldiagnosis`.`createuser`,
    `refprovisionaldiagnosis`.`description`,
    `refprovisionaldiagnosis`.`groupsortorder`,
    `refprovisionaldiagnosis`.`orgid`,
    `refprovisionaldiagnosis`.`provisionaldiagnosiscode`,
    `refprovisionaldiagnosis`.`provisionaldiagnosisname`,
    `refprovisionaldiagnosis`.`refprovisionaldiagnosisgroupid`,
    `refprovisionaldiagnosis`.`refprovisionaldiagnosisid`,
    `refprovisionaldiagnosis`.`sortorder`,
    `refprovisionaldiagnosis`.`status`,
    `refprovisionaldiagnosis`.`updatedate`,
    `refprovisionaldiagnosis`.`updateuser`

FROM
    {{ source("bay_dbo", "refprovisionaldiagnosis") }} AS `refprovisionaldiagnosis`
