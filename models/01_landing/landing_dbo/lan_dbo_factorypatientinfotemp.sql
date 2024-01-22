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
            "`age`",
            "`age_0_1_years`",
            "`age_1_5_years`",
            "`age_over_5_years`",
            "`birth_date`",
            "`company_name`",
            "`country`",
            "`department`",
            "`designation`",
            "`district`",
            "`education`",
            "`emailaddress`",
            "`employee_id`",
            "`family_name`",
            "`father_or_husband_name`",
            "`gender`",
            "`given_name`",
            "`head_of_family`",
            "`house_no`",
            "`joiningdate`",
            "`marital_status`",
            "`mobile_no`",
            "`mother_name`",
            "`national_id_no`",
            "`no_of_family_member`",
            "`religion`",
            "`road_no`",
            "`thana`",
            "`village`"
        ])
    }} AS `ingestion_sk`,
    `age`,
    `age_0_1_years`,
    `age_1_5_years`,
    `age_over_5_years`,
    `birth_date`,
    `company_name`,
    `country`,
    `department`,
    `designation`,
    `district`,
    `education`,
    `emailaddress`,
    `employee_id`,
    `family_name`,
    `father_or_husband_name`,
    `gender`,
    `given_name`,
    `head_of_family`,
    `house_no`,
    `joiningdate`,
    `marital_status`,
    `mobile_no`,
    `mother_name`,
    `national_id_no`,
    `no_of_family_member`,
    `religion`,
    `road_no`,
    `thana`,
    `village`

FROM
    {{ source("bay_dbo", "factorypatientinfotemp") }}
