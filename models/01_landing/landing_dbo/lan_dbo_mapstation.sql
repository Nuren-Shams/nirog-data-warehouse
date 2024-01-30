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
            "`mapstation`.`createdate`",
            "`mapstation`.`createuser`",
            "`mapstation`.`patientid`",
            "`mapstation`.`stationid`",
            "`mapstation`.`stationstatus`",
            "`mapstation`.`updatedate`",
            "`mapstation`.`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `mapstation`.`createdate`,
    `mapstation`.`createuser`,
    `mapstation`.`patientid`,
    `mapstation`.`stationid`,
    `mapstation`.`stationstatus`,
    `mapstation`.`updatedate`,
    `mapstation`.`updateuser`

FROM
    {{ source("bay_dbo", "mapstation") }} AS `mapstation`
