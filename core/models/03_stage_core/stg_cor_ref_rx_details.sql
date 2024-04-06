{{-
    config(
        materialized = "table",
        tags = ["execute_daily", "ref", "rx_details"]
    )
-}}

SELECT DISTINCT
    mdrxd.rx_name

FROM
    {{ ref("bse_dbo_mdata_rx_details") }} AS mdrxd
