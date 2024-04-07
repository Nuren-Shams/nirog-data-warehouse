{{-
    config(
        materialized = "incremental",
        unique_key = "ingestion_sk",
        tags = ["execute_daily"]
    )
-}}


SELECT DISTINCT
    {{
        dbt_utils.generate_surrogate_key([
            "`med__htn_dm__cxb_noakhali`.`generic_name`",
            "`med__htn_dm__cxb_noakhali`.`trade_name`",
            "`med__htn_dm__cxb_noakhali`.`type`",
            "`med__htn_dm__cxb_noakhali`.`strength`",
            "`med__htn_dm__cxb_noakhali`.`formation`",
            "`med__htn_dm__cxb_noakhali`.`contraindicated_in_preganancy`"
        ])
    }} AS `ingestion_sk`,
    `med__htn_dm__cxb_noakhali`.`generic_name`,
    `med__htn_dm__cxb_noakhali`.`trade_name`,
    `med__htn_dm__cxb_noakhali`.`type`,
    `med__htn_dm__cxb_noakhali`.`strength`,
    `med__htn_dm__cxb_noakhali`.`formation`,
    `med__htn_dm__cxb_noakhali`.`contraindicated_in_preganancy`

FROM
    {{ source("bay_external", "med__htn_dm__cxb_noakhali") }} AS `med__htn_dm__cxb_noakhali`
