{{-
    config(
        materialized = "table",
        tags = ["execute_daily"]
    )
-}}

SELECT 
    IF(UPPER(generic_name) IN ("NONE", ""), NULL, UPPER(generic_name)) AS generic_name,
    IF(UPPER(trade_name) IN ("NONE", ""), NULL, UPPER(trade_name)) AS trade_name,
    IF(UPPER(type) IN ("NONE", ""), NULL, UPPER(type)) AS type,
    IF(UPPER(strength) IN ("NONE", ""), NULL, UPPER(strength)) AS strength,
    IF(UPPER(formation) IN ("NONE", ""), NULL, UPPER(formation)) AS formation,
    IF(UPPER(contraindicated_in_preganancy) IN ("NONE", ""), NULL, UPPER(contraindicated_in_preganancy)) AS contraindicated_in_preganancy

FROM 
    {{ ref("lan_ext_med__htn_dm__cxb_noakhali") }}
