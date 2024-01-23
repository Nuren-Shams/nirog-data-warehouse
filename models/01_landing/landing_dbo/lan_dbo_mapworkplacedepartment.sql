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
            "`mapworkplacedepartment`.`createdate`",
            "`mapworkplacedepartment`.`createuser`",
            "`mapworkplacedepartment`.`mappingworkplacedepartmentid`",
            "`mapworkplacedepartment`.`orgid`",
            "`mapworkplacedepartment`.`refdepartmentid`",
            "`mapworkplacedepartment`.`status`",
            "`mapworkplacedepartment`.`updatedate`",
            "`mapworkplacedepartment`.`updateuser`",
            "`mapworkplacedepartment`.`workplaceid`"
        ])
    }} AS `ingestion_sk`,
    `mapworkplacedepartment`.`createdate`,
    `mapworkplacedepartment`.`createuser`,
    `mapworkplacedepartment`.`mappingworkplacedepartmentid`,
    `mapworkplacedepartment`.`orgid`,
    `mapworkplacedepartment`.`refdepartmentid`,
    `mapworkplacedepartment`.`status`,
    `mapworkplacedepartment`.`updatedate`,
    `mapworkplacedepartment`.`updateuser`,
    `mapworkplacedepartment`.`workplaceid`

FROM
    {{ source("bay_dbo", "mapworkplacedepartment") }} AS `mapworkplacedepartment`
