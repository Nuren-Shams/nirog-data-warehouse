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
            "id",
            "orgid",
            "shortname",
            "status",
            "unionname",
            "upazilaid",
            "updatedate",
            "updateuser"
        ])
    }} AS ingestion_sk,
    createdate,
    createuser,
    id,
    orgid,
    shortname,
    status,
    unionname,
    upazilaid,
    updatedate,
    updateuser

FROM
    {{ source("bay_dbo", "union") }}
