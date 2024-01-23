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
            "`smssenddata`.`banglaunicode`",
            "`smssenddata`.`createdate`",
            "`smssenddata`.`createuser`",
            "`smssenddata`.`isbanglasmsbody`",
            "`smssenddata`.`issend`",
            "`smssenddata`.`mobileno`",
            "`smssenddata`.`orgid`",
            "`smssenddata`.`patientid`",
            "`smssenddata`.`smsbody`",
            "`smssenddata`.`smsresponse`",
            "`smssenddata`.`smssenddataid`"
        ])
    }} AS `ingestion_sk`,
    `smssenddata`.`banglaunicode`,
    `smssenddata`.`createdate`,
    `smssenddata`.`createuser`,
    `smssenddata`.`isbanglasmsbody`,
    `smssenddata`.`issend`,
    `smssenddata`.`mobileno`,
    `smssenddata`.`orgid`,
    `smssenddata`.`patientid`,
    `smssenddata`.`smsbody`,
    `smssenddata`.`smsresponse`,
    `smssenddata`.`smssenddataid`

FROM
    {{ source("bay_dbo", "smssenddata") }} AS `smssenddata`
