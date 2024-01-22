{{
    config(
        materialized = "incremental",
        unique_key = "ingestion_sk",
        tags = ["execute_daily"]
    )
}}


SELECT
    {{
        dbt_utils.generate_surrogate_key([
            "collectiondate",
            "comment",
            "contenttype",
            "createdate",
            "createuser",
            "filedata",
            "filelocation",
            "filename",
            "filesize",
            "filetypeid",
            "fileuploaddate",
            "isfileinanotherlocation",
            "mdatapatientreferralbackfileid",
            "mdatapatientreferralbackid",
            "orgid",
            "patientid",
            "sortorder",
            "status",
            "updatedate",
            "updateuser"
        ])
    }} AS ingestion_sk,
    collectiondate,
    comment,
    contenttype,
    createdate,
    createuser,
    filedata,
    filelocation,
    filename,
    filesize,
    filetypeid,
    fileuploaddate,
    isfileinanotherlocation,
    mdatapatientreferralbackfileid,
    mdatapatientreferralbackid,
    orgid,
    patientid,
    sortorder,
    status,
    updatedate,
    updateuser

FROM
    {{ source("bay_dbo", "mdatapatientreferralbackfile") }}
