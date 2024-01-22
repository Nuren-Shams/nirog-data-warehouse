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
            "`correlationid`",
            "`createdate`",
            "`createuser`",
            "`eventid`",
            "`eventname`",
            "`fieldlong1`",
            "`fieldlong2`",
            "`fieldlong3`",
            "`fieldshort1`",
            "`fieldshort2`",
            "`fieldshort3`",
            "`logcategory`",
            "`orgid`",
            "`processtime`",
            "`requestdatetime`",
            "`severity`",
            "`severityrank`",
            "`status`",
            "`updatedate`",
            "`updateuser`",
            "`violationcode`",
            "`webapilogdetailid`"
        ])
    }} AS `ingestion_sk`,
    `correlationid`,
    `createdate`,
    `createuser`,
    `eventid`,
    `eventname`,
    `fieldlong1`,
    `fieldlong2`,
    `fieldlong3`,
    `fieldshort1`,
    `fieldshort2`,
    `fieldshort3`,
    `logcategory`,
    `orgid`,
    `processtime`,
    `requestdatetime`,
    `severity`,
    `severityrank`,
    `status`,
    `updatedate`,
    `updateuser`,
    `violationcode`,
    `webapilogdetailid`

FROM
    {{ source("bay_dbo", "webapilogdetail") }}
