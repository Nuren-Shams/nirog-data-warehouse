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
            "description",
            "orgid",
            "sortorder",
            "status",
            "tbhistoryid",
            "tbhistoryidcode",
            "tbpasthistoryanswer1",
            "tbpasthistoryanswer2",
            "tbpasthistoryanswer3",
            "tbpasthistoryothers1",
            "tbpasthistoryothers2",
            "tbpasthistoryquestion",
            "updatedate",
            "updateuser"
        ])
    }} AS ingestion_sk,
    createdate,
    createuser,
    description,
    orgid,
    sortorder,
    status,
    tbhistoryid,
    tbhistoryidcode,
    tbpasthistoryanswer1,
    tbpasthistoryanswer2,
    tbpasthistoryanswer3,
    tbpasthistoryothers1,
    tbpasthistoryothers2,
    tbpasthistoryquestion,
    updatedate,
    updateuser

FROM
    {{ source("bay_dbo", "reftbpasthistory") }}
