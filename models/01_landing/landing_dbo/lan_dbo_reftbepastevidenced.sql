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
            "`reftbepastevidenced`.`createdate`",
            "`reftbepastevidenced`.`createuser`",
            "`reftbepastevidenced`.`description`",
            "`reftbepastevidenced`.`orgid`",
            "`reftbepastevidenced`.`sortorder`",
            "`reftbepastevidenced`.`status`",
            "`reftbepastevidenced`.`tbepastevidencecode`",
            "`reftbepastevidenced`.`tbepastevidenceid`",
            "`reftbepastevidenced`.`updatedate`",
            "`reftbepastevidenced`.`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `reftbepastevidenced`.`createdate`,
    `reftbepastevidenced`.`createuser`,
    `reftbepastevidenced`.`description`,
    `reftbepastevidenced`.`orgid`,
    `reftbepastevidenced`.`sortorder`,
    `reftbepastevidenced`.`status`,
    `reftbepastevidenced`.`tbepastevidencecode`,
    `reftbepastevidenced`.`tbepastevidenceid`,
    `reftbepastevidenced`.`updatedate`,
    `reftbepastevidenced`.`updateuser`

FROM
    {{ source("bay_dbo", "reftbepastevidenced") }} AS `reftbepastevidenced`
