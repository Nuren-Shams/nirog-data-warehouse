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
            "createdate",
            "createuser",
            "mapmdatainvestigationmdatapatientfileid",
            "mdinvestigationid",
            "mdpatientfileid",
            "orgid",
            "status",
            "updatedate",
            "updateuser"
        ])
    }} AS ingestion_sk,
    createdate,
    createuser,
    mapmdatainvestigationmdatapatientfileid,
    mdinvestigationid,
    mdpatientfileid,
    orgid,
    status,
    updatedate,
    updateuser

FROM
    {{ source("bay_dbo", "mapmdatainvestigationmdatapatientfile") }}
