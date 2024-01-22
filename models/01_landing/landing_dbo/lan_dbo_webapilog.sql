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
            "`actionname`",
            "`apiversion`",
            "`authorizationheader`",
            "`authorizationtype`",
            "`correlationid`",
            "`createdate`",
            "`createuser`",
            "`eventid`",
            "`eventname`",
            "`externalcorrelationid`",
            "`httpmethod`",
            "`logcategory`",
            "`orgid`",
            "`originalrequestip`",
            "`processtime`",
            "`requestbody`",
            "`requestdatetime`",
            "`requestenddatetime`",
            "`requestheaders`",
            "`requestipaddress`",
            "`requeststartdatetime`",
            "`responsebody`",
            "`responsecode`",
            "`responseheaders`",
            "`routeid`",
            "`severity`",
            "`severityrank`",
            "`status`",
            "`updatedate`",
            "`updateuser`",
            "`uri`",
            "`uriparameters`",
            "`webapiserver`"
        ])
    }} AS `ingestion_sk`,
    `actionname`,
    `apiversion`,
    `authorizationheader`,
    `authorizationtype`,
    `correlationid`,
    `createdate`,
    `createuser`,
    `eventid`,
    `eventname`,
    `externalcorrelationid`,
    `httpmethod`,
    `logcategory`,
    `orgid`,
    `originalrequestip`,
    `processtime`,
    `requestbody`,
    `requestdatetime`,
    `requestenddatetime`,
    `requestheaders`,
    `requestipaddress`,
    `requeststartdatetime`,
    `responsebody`,
    `responsecode`,
    `responseheaders`,
    `routeid`,
    `severity`,
    `severityrank`,
    `status`,
    `updatedate`,
    `updateuser`,
    `uri`,
    `uriparameters`,
    `webapiserver`

FROM
    {{ source("bay_dbo", "webapilog") }}
