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
            "`mapmdatainvestigationmdatapatientfile`.`createdate`",
            "`mapmdatainvestigationmdatapatientfile`.`createuser`",
            "`mapmdatainvestigationmdatapatientfile`.`mapmdatainvestigationmdatapatientfileid`",
            "`mapmdatainvestigationmdatapatientfile`.`mdinvestigationid`",
            "`mapmdatainvestigationmdatapatientfile`.`mdpatientfileid`",
            "`mapmdatainvestigationmdatapatientfile`.`orgid`",
            "`mapmdatainvestigationmdatapatientfile`.`status`",
            "`mapmdatainvestigationmdatapatientfile`.`updatedate`",
            "`mapmdatainvestigationmdatapatientfile`.`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `mapmdatainvestigationmdatapatientfile`.`createdate`,
    `mapmdatainvestigationmdatapatientfile`.`createuser`,
    `mapmdatainvestigationmdatapatientfile`.`mapmdatainvestigationmdatapatientfileid`,
    `mapmdatainvestigationmdatapatientfile`.`mdinvestigationid`,
    `mapmdatainvestigationmdatapatientfile`.`mdpatientfileid`,
    `mapmdatainvestigationmdatapatientfile`.`orgid`,
    `mapmdatainvestigationmdatapatientfile`.`status`,
    `mapmdatainvestigationmdatapatientfile`.`updatedate`,
    `mapmdatainvestigationmdatapatientfile`.`updateuser`

FROM
    {{ source("bay_dbo", "mapmdatainvestigationmdatapatientfile") }} AS `mapmdatainvestigationmdatapatientfile`
