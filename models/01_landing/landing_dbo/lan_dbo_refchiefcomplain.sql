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
            "refchiefcomplain.cccode",
            "refchiefcomplain.ccid",
            "refchiefcomplain.createdate",
            "refchiefcomplain.createuser",
            "refchiefcomplain.description",
            "refchiefcomplain.orgid",
            "refchiefcomplain.sortorder",
            "refchiefcomplain.status",
            "refchiefcomplain.updatedate",
            "refchiefcomplain.updateuser"
        ])
    }} AS ingestion_sk,
    refchiefcomplain.cccode,
    refchiefcomplain.ccid,
    refchiefcomplain.createdate,
    refchiefcomplain.createuser,
    refchiefcomplain.description,
    refchiefcomplain.orgid,
    refchiefcomplain.sortorder,
    refchiefcomplain.status,
    refchiefcomplain.updatedate,
    refchiefcomplain.updateuser

FROM
    {{ source("bay_dbo", "refchiefcomplain") }} AS refchiefcomplain
