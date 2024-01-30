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
            "`reflabinvestigationgroup`.`createdate`",
            "`reflabinvestigationgroup`.`createuser`",
            "`reflabinvestigationgroup`.`description`",
            "`reflabinvestigationgroup`.`orgid`",
            "`reflabinvestigationgroup`.`reflabinvestigationgroupcode`",
            "`reflabinvestigationgroup`.`reflabinvestigationgroupid`",
            "`reflabinvestigationgroup`.`sortorder`",
            "`reflabinvestigationgroup`.`status`",
            "`reflabinvestigationgroup`.`updatedate`",
            "`reflabinvestigationgroup`.`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `reflabinvestigationgroup`.`createdate`,
    `reflabinvestigationgroup`.`createuser`,
    `reflabinvestigationgroup`.`description`,
    `reflabinvestigationgroup`.`orgid`,
    `reflabinvestigationgroup`.`reflabinvestigationgroupcode`,
    `reflabinvestigationgroup`.`reflabinvestigationgroupid`,
    `reflabinvestigationgroup`.`sortorder`,
    `reflabinvestigationgroup`.`status`,
    `reflabinvestigationgroup`.`updatedate`,
    `reflabinvestigationgroup`.`updateuser`

FROM
    {{ source("bay_dbo", "reflabinvestigationgroup") }} AS `reflabinvestigationgroup`
