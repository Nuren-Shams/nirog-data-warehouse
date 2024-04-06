{{-
    config(
        materialized = "table",
        tags = ["execute_daily", "mdata", "rx_details", "extended"]
    )
-}}

SELECT
    mdrxd.patient_id,
    mdrxd.collected_date,
    REGEXP_REPLACE(
        TRIM(
            STRING_AGG(
                CONCAT(
                    "Drug Name: ", COALESCE(CAST(mdrxd.rx_name AS STRING), "-"), ", ",
                    "Drug Dose: ", COALESCE(mdrxd.rx_dose, "-"), ", ",
                    "Frequency Hour: ", COALESCE(mdrxd.rx_frequency_hour, "-"), ", ",
                    "Rx Duration: ", COALESCE(mdrxd.rx_duration_value, "-")
                ), ";\n"
            )
         ), R"\s+", " "
     ) AS rx_details

FROM
    {{ ref("bse_dbo_mdata_rx_details") }} AS mdrxd

GROUP BY
    patient_id,
    collected_date
