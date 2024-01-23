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
            "`webapilogdetail`.`correlationid`",
            "`webapilogdetail`.`createdate`",
            "`webapilogdetail`.`createuser`",
            "`webapilogdetail`.`eventid`",
            "`webapilogdetail`.`eventname`",
            "`webapilogdetail`.`fieldlong1`",
            "`webapilogdetail`.`fieldlong2`",
            "`webapilogdetail`.`fieldlong3`",
            "`webapilogdetail`.`fieldshort1`",
            "`webapilogdetail`.`fieldshort2`",
            "`webapilogdetail`.`fieldshort3`",
            "`webapilogdetail`.`logcategory`",
            "`webapilogdetail`.`orgid`",
            "`webapilogdetail`.`processtime`",
            "`webapilogdetail`.`requestdatetime`",
            "`webapilogdetail`.`severity`",
            "`webapilogdetail`.`severityrank`",
            "`webapilogdetail`.`status`",
            "`webapilogdetail`.`updatedate`",
            "`webapilogdetail`.`updateuser`",
            "`webapilogdetail`.`violationcode`",
            "`webapilogdetail`.`webapilogdetailid`"
        ])
    }} AS `ingestion_sk`,
    `webapilogdetail`.`correlationid`,
    `webapilogdetail`.`createdate`,
    `webapilogdetail`.`createuser`,
    `webapilogdetail`.`eventid`,
    `webapilogdetail`.`eventname`,
    `webapilogdetail`.`fieldlong1`,
    `webapilogdetail`.`fieldlong2`,
    `webapilogdetail`.`fieldlong3`,
    `webapilogdetail`.`fieldshort1`,
    `webapilogdetail`.`fieldshort2`,
    `webapilogdetail`.`fieldshort3`,
    `webapilogdetail`.`logcategory`,
    `webapilogdetail`.`orgid`,
    `webapilogdetail`.`processtime`,
    `webapilogdetail`.`requestdatetime`,
    `webapilogdetail`.`severity`,
    `webapilogdetail`.`severityrank`,
    `webapilogdetail`.`status`,
    `webapilogdetail`.`updatedate`,
    `webapilogdetail`.`updateuser`,
    `webapilogdetail`.`violationcode`,
    `webapilogdetail`.`webapilogdetailid`

FROM
    {{ source("bay_dbo", "webapilogdetail") }} AS `webapilogdetail`
