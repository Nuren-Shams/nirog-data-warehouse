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
            "`mdataphysicalfinding`.`collectiondate`",
            "`mdataphysicalfinding`.`createdate`",
            "`mdataphysicalfinding`.`createuser`",
            "`mdataphysicalfinding`.`mdphysicalfindingid`",
            "`mdataphysicalfinding`.`orgid`",
            "`mdataphysicalfinding`.`patientid`",
            "`mdataphysicalfinding`.`physicalfinding`",
            "`mdataphysicalfinding`.`status`",
            "`mdataphysicalfinding`.`updatedate`",
            "`mdataphysicalfinding`.`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `mdataphysicalfinding`.`collectiondate`,
    `mdataphysicalfinding`.`createdate`,
    `mdataphysicalfinding`.`createuser`,
    `mdataphysicalfinding`.`mdphysicalfindingid`,
    `mdataphysicalfinding`.`orgid`,
    `mdataphysicalfinding`.`patientid`,
    `mdataphysicalfinding`.`physicalfinding`,
    `mdataphysicalfinding`.`status`,
    `mdataphysicalfinding`.`updatedate`,
    `mdataphysicalfinding`.`updateuser`

FROM
    {{ source("bay_dbo", "mdataphysicalfinding") }} AS `mdataphysicalfinding`
