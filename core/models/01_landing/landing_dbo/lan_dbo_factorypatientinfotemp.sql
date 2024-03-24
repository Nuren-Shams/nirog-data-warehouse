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
            "`factorypatientinfotemp`.`age`",
            "`factorypatientinfotemp`.`age_0_1_years`",
            "`factorypatientinfotemp`.`age_1_5_years`",
            "`factorypatientinfotemp`.`age_over_5_years`",
            "`factorypatientinfotemp`.`birth_date`",
            "`factorypatientinfotemp`.`company_name`",
            "`factorypatientinfotemp`.`country`",
            "`factorypatientinfotemp`.`department`",
            "`factorypatientinfotemp`.`designation`",
            "`factorypatientinfotemp`.`district`",
            "`factorypatientinfotemp`.`education`",
            "`factorypatientinfotemp`.`emailaddress`",
            "`factorypatientinfotemp`.`employee_id`",
            "`factorypatientinfotemp`.`family_name`",
            "`factorypatientinfotemp`.`father_or_husband_name`",
            "`factorypatientinfotemp`.`gender`",
            "`factorypatientinfotemp`.`given_name`",
            "`factorypatientinfotemp`.`head_of_family`",
            "`factorypatientinfotemp`.`house_no`",
            "`factorypatientinfotemp`.`joiningdate`",
            "`factorypatientinfotemp`.`marital_status`",
            "`factorypatientinfotemp`.`mobile_no`",
            "`factorypatientinfotemp`.`mother_name`",
            "`factorypatientinfotemp`.`national_id_no`",
            "`factorypatientinfotemp`.`no_of_family_member`",
            "`factorypatientinfotemp`.`religion`",
            "`factorypatientinfotemp`.`road_no`",
            "`factorypatientinfotemp`.`thana`",
            "`factorypatientinfotemp`.`village`"
        ])
    }} AS `ingestion_sk`,
    `factorypatientinfotemp`.`age`,
    `factorypatientinfotemp`.`age_0_1_years`,
    `factorypatientinfotemp`.`age_1_5_years`,
    `factorypatientinfotemp`.`age_over_5_years`,
    `factorypatientinfotemp`.`birth_date`,
    `factorypatientinfotemp`.`company_name`,
    `factorypatientinfotemp`.`country`,
    `factorypatientinfotemp`.`department`,
    `factorypatientinfotemp`.`designation`,
    `factorypatientinfotemp`.`district`,
    `factorypatientinfotemp`.`education`,
    `factorypatientinfotemp`.`emailaddress`,
    `factorypatientinfotemp`.`employee_id`,
    `factorypatientinfotemp`.`family_name`,
    `factorypatientinfotemp`.`father_or_husband_name`,
    `factorypatientinfotemp`.`gender`,
    `factorypatientinfotemp`.`given_name`,
    `factorypatientinfotemp`.`head_of_family`,
    `factorypatientinfotemp`.`house_no`,
    `factorypatientinfotemp`.`joiningdate`,
    `factorypatientinfotemp`.`marital_status`,
    `factorypatientinfotemp`.`mobile_no`,
    `factorypatientinfotemp`.`mother_name`,
    `factorypatientinfotemp`.`national_id_no`,
    `factorypatientinfotemp`.`no_of_family_member`,
    `factorypatientinfotemp`.`religion`,
    `factorypatientinfotemp`.`road_no`,
    `factorypatientinfotemp`.`thana`,
    `factorypatientinfotemp`.`village`

FROM
    {{ source("bay_dbo", "factorypatientinfotemp") }} AS `factorypatientinfotemp`
