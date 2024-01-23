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
            "mdatapatientfile.collectiondate",
            "mdatapatientfile.comment",
            "mdatapatientfile.contenttype",
            "mdatapatientfile.createdate",
            "mdatapatientfile.createuser",
            "mdatapatientfile.filedata",
            "mdatapatientfile.filelocation",
            "mdatapatientfile.filename",
            "mdatapatientfile.filesize",
            "mdatapatientfile.filetypeid",
            "mdatapatientfile.fileuploaddate",
            "mdatapatientfile.isfileinanotherlocation",
            "mdatapatientfile.mappingid",
            "mdatapatientfile.mappingtablename",
            "mdatapatientfile.mdpatientfileid",
            "mdatapatientfile.orgid",
            "mdatapatientfile.patientid",
            "mdatapatientfile.sortorder",
            "mdatapatientfile.status",
            "mdatapatientfile.updatedate",
            "mdatapatientfile.updateuser"
        ])
    }} AS ingestion_sk,
    mdatapatientfile.collectiondate,
    mdatapatientfile.comment,
    mdatapatientfile.contenttype,
    mdatapatientfile.createdate,
    mdatapatientfile.createuser,
    mdatapatientfile.filedata,
    mdatapatientfile.filelocation,
    mdatapatientfile.filename,
    mdatapatientfile.filesize,
    mdatapatientfile.filetypeid,
    mdatapatientfile.fileuploaddate,
    mdatapatientfile.isfileinanotherlocation,
    mdatapatientfile.mappingid,
    mdatapatientfile.mappingtablename,
    mdatapatientfile.mdpatientfileid,
    mdatapatientfile.orgid,
    mdatapatientfile.patientid,
    mdatapatientfile.sortorder,
    mdatapatientfile.status,
    mdatapatientfile.updatedate,
    mdatapatientfile.updateuser

FROM
    {{ source("bay_dbo", "mdatapatientfile") }} AS mdatapatientfile
