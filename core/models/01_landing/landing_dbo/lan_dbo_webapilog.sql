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
            "`webapilog`.`actionname`",
            "`webapilog`.`apiversion`",
            "`webapilog`.`authorizationheader`",
            "`webapilog`.`authorizationtype`",
            "`webapilog`.`correlationid`",
            "`webapilog`.`createdate`",
            "`webapilog`.`createuser`",
            "`webapilog`.`eventid`",
            "`webapilog`.`eventname`",
            "`webapilog`.`externalcorrelationid`",
            "`webapilog`.`httpmethod`",
            "`webapilog`.`logcategory`",
            "`webapilog`.`orgid`",
            "`webapilog`.`originalrequestip`",
            "`webapilog`.`processtime`",
            "`webapilog`.`requestbody`",
            "`webapilog`.`requestdatetime`",
            "`webapilog`.`requestenddatetime`",
            "`webapilog`.`requestheaders`",
            "`webapilog`.`requestipaddress`",
            "`webapilog`.`requeststartdatetime`",
            "`webapilog`.`responsebody`",
            "`webapilog`.`responsecode`",
            "`webapilog`.`responseheaders`",
            "`webapilog`.`routeid`",
            "`webapilog`.`severity`",
            "`webapilog`.`severityrank`",
            "`webapilog`.`status`",
            "`webapilog`.`updatedate`",
            "`webapilog`.`updateuser`",
            "`webapilog`.`uri`",
            "`webapilog`.`uriparameters`",
            "`webapilog`.`webapiserver`"
        ])
    }} AS `ingestion_sk`,
    `webapilog`.`actionname`,
    `webapilog`.`apiversion`,
    `webapilog`.`authorizationheader`,
    `webapilog`.`authorizationtype`,
    `webapilog`.`correlationid`,
    `webapilog`.`createdate`,
    `webapilog`.`createuser`,
    `webapilog`.`eventid`,
    `webapilog`.`eventname`,
    `webapilog`.`externalcorrelationid`,
    `webapilog`.`httpmethod`,
    `webapilog`.`logcategory`,
    `webapilog`.`orgid`,
    `webapilog`.`originalrequestip`,
    `webapilog`.`processtime`,
    `webapilog`.`requestbody`,
    `webapilog`.`requestdatetime`,
    `webapilog`.`requestenddatetime`,
    `webapilog`.`requestheaders`,
    `webapilog`.`requestipaddress`,
    `webapilog`.`requeststartdatetime`,
    `webapilog`.`responsebody`,
    `webapilog`.`responsecode`,
    `webapilog`.`responseheaders`,
    `webapilog`.`routeid`,
    `webapilog`.`severity`,
    `webapilog`.`severityrank`,
    `webapilog`.`status`,
    `webapilog`.`updatedate`,
    `webapilog`.`updateuser`,
    `webapilog`.`uri`,
    `webapilog`.`uriparameters`,
    `webapilog`.`webapiserver`

FROM
    {{ source("bay_dbo", "webapilog") }} AS `webapilog`
