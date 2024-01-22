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
            "`banglaunicode`",
            "`createdate`",
            "`createuser`",
            "`extrajson`",
            "`hasmobileno`",
            "`isbanglasmsbody`",
            "`ispulled`",
            "`isrunimmediately`",
            "`issendall`",
            "`mobileno`",
            "`orgid`",
            "`senddate`",
            "`smsbody`",
            "`smsondemandid`",
            "`smssubject`",
            "`status`",
            "`updatedate`",
            "`updateuser`",
            "`workplacebranchid`",
            "`workplaceid`"
        ])
    }} AS `ingestion_sk`,
    `banglaunicode`,
    `createdate`,
    `createuser`,
    `extrajson`,
    `hasmobileno`,
    `isbanglasmsbody`,
    `ispulled`,
    `isrunimmediately`,
    `issendall`,
    `mobileno`,
    `orgid`,
    `senddate`,
    `smsbody`,
    `smsondemandid`,
    `smssubject`,
    `status`,
    `updatedate`,
    `updateuser`,
    `workplacebranchid`,
    `workplaceid`

FROM
    {{ source("bay_dbo", "smsondemand") }}
