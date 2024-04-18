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
            "`refapifacility`.`create_date`",
            "`refapifacility`.`create_user`",
            "`refapifacility`.`district`",
            "`refapifacility`.`enable_diabetes_management`",
            "`refapifacility`.`enable_monthly_screening_reports`",
            "`refapifacility`.`enable_monthly_supplies_reports`",
            "`refapifacility`.`facility_group`",
            "`refapifacility`.`facility_name`",
            "`refapifacility`.`facility_short_name`",
            "`refapifacility`.`facility_type`",
            "`refapifacility`.`id`",
            "`refapifacility`.`identifier`",
            "`refapifacility`.`latitude`",
            "`refapifacility`.`longitude`",
            "`refapifacility`.`org_id`",
            "`refapifacility`.`organization`",
            "`refapifacility`.`pin`",
            "`refapifacility`.`size`",
            "`refapifacility`.`street_address`",
            "`refapifacility`.`update_date`",
            "`refapifacility`.`update_user`",
            "`refapifacility`.`village_or_colony`",
            "`refapifacility`.`zone_or_block`"
        ])
    }} AS `ingestion_sk`,
    `refapifacility`.`create_date`,
    `refapifacility`.`create_user`,
    `refapifacility`.`district`,
    `refapifacility`.`enable_diabetes_management`,
    `refapifacility`.`enable_monthly_screening_reports`,
    `refapifacility`.`enable_monthly_supplies_reports`,
    `refapifacility`.`facility_group`,
    `refapifacility`.`facility_name`,
    `refapifacility`.`facility_short_name`,
    `refapifacility`.`facility_type`,
    `refapifacility`.`id`,
    `refapifacility`.`identifier`,
    `refapifacility`.`latitude`,
    `refapifacility`.`longitude`,
    `refapifacility`.`org_id`,
    `refapifacility`.`organization`,
    `refapifacility`.`pin`,
    `refapifacility`.`size`,
    `refapifacility`.`street_address`,
    `refapifacility`.`update_date`,
    `refapifacility`.`update_user`,
    `refapifacility`.`village_or_colony`,
    `refapifacility`.`zone_or_block`

FROM
    {{ source("bay_dbo", "refapifacility") }} AS `refapifacility`
