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
            "`upazila`.`createdate`",
            "`upazila`.`createuser`",
            "`upazila`.`districtid`",
            "`upazila`.`id`",
            "`upazila`.`orgid`",
            "`upazila`.`shortname`",
            "`upazila`.`status`",
            "`upazila`.`upazilaname`",
            "`upazila`.`updatedate`",
            "`upazila`.`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `upazila`.`createdate`,
    `upazila`.`createuser`,
    `upazila`.`districtid`,
    `upazila`.`id`,
    `upazila`.`orgid`,
    `upazila`.`shortname`,
    `upazila`.`status`,
    `upazila`.`upazilaname`,
    `upazila`.`updatedate`,
    `upazila`.`updateuser`

FROM
    {{ source("bay_dbo", "upazila") }} AS `upazila`
