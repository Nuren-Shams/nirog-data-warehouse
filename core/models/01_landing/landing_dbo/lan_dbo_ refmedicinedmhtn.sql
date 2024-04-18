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
            "`refmedicinedmhtn`.`createdate`",
            "`refmedicinedmhtn`.`createuser`",
            "`refmedicinedmhtn`.`dragformation`",
            "`refmedicinedmhtn`.`draggroup`",
            "`refmedicinedmhtn`.`dragid`",
            "`refmedicinedmhtn`.`dragmg`",
            "`refmedicinedmhtn`.`dragname`",
            "`refmedicinedmhtn`.`dragtype`",
            "`refmedicinedmhtn`.`id`",
            "`refmedicinedmhtn`.`orgid`",
            "`refmedicinedmhtn`.`updatedate`",
            "`refmedicinedmhtn`.`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `refmedicinedmhtn`.`createdate`,
    `refmedicinedmhtn`.`createuser`,
    `refmedicinedmhtn`.`dragformation`,
    `refmedicinedmhtn`.`draggroup`,
    `refmedicinedmhtn`.`dragid`,
    `refmedicinedmhtn`.`dragmg`,
    `refmedicinedmhtn`.`dragname`,
    `refmedicinedmhtn`.`dragtype`,
    `refmedicinedmhtn`.`id`,
    `refmedicinedmhtn`.`orgid`,
    `refmedicinedmhtn`.`updatedate`,
    `refmedicinedmhtn`.`updateuser`

FROM
    {{ source("bay_dbo", "refmedicinedmhtn") }} AS `refmedicinedmhtn`
