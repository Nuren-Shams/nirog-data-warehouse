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
            "`allergytomedication`",
            "`collectiondate`",
            "`createdate`",
            "`createuser`",
            "`dose`",
            "`drugid`",
            "`durationid`",
            "`frequencyhour`",
            "`orgid`",
            "`patientid`",
            "`rx`",
            "`rxdurationvalue`",
            "`rxid`",
            "`status`",
            "`updatedate`",
            "`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `allergytomedication`,
    `collectiondate`,
    `createdate`,
    `createuser`,
    `dose`,
    `drugid`,
    `durationid`,
    `frequencyhour`,
    `orgid`,
    `patientid`,
    `rx`,
    `rxdurationvalue`,
    `rxid`,
    `status`,
    `updatedate`,
    `updateuser`

FROM
    {{ source("bay_dbo", "mdatarxdetails") }}
