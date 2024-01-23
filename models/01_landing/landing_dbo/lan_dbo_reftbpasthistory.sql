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
            "reftbpasthistory.createdate",
            "reftbpasthistory.createuser",
            "reftbpasthistory.description",
            "reftbpasthistory.orgid",
            "reftbpasthistory.sortorder",
            "reftbpasthistory.status",
            "reftbpasthistory.tbhistoryid",
            "reftbpasthistory.tbhistoryidcode",
            "reftbpasthistory.tbpasthistoryanswer1",
            "reftbpasthistory.tbpasthistoryanswer2",
            "reftbpasthistory.tbpasthistoryanswer3",
            "reftbpasthistory.tbpasthistoryothers1",
            "reftbpasthistory.tbpasthistoryothers2",
            "reftbpasthistory.tbpasthistoryquestion",
            "reftbpasthistory.updatedate",
            "reftbpasthistory.updateuser"
        ])
    }} AS ingestion_sk,
    reftbpasthistory.createdate,
    reftbpasthistory.createuser,
    reftbpasthistory.description,
    reftbpasthistory.orgid,
    reftbpasthistory.sortorder,
    reftbpasthistory.status,
    reftbpasthistory.tbhistoryid,
    reftbpasthistory.tbhistoryidcode,
    reftbpasthistory.tbpasthistoryanswer1,
    reftbpasthistory.tbpasthistoryanswer2,
    reftbpasthistory.tbpasthistoryanswer3,
    reftbpasthistory.tbpasthistoryothers1,
    reftbpasthistory.tbpasthistoryothers2,
    reftbpasthistory.tbpasthistoryquestion,
    reftbpasthistory.updatedate,
    reftbpasthistory.updateuser

FROM
    {{ source("bay_dbo", "reftbpasthistory") }} AS reftbpasthistory
