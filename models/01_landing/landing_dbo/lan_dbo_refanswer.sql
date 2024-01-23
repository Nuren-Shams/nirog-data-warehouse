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
            "refanswer.answergroupid",
            "refanswer.answerid",
            "refanswer.answermodulename",
            "refanswer.answertitle",
            "refanswer.buttontype",
            "refanswer.createdate",
            "refanswer.createuser",
            "refanswer.description",
            "refanswer.orgid",
            "refanswer.sortorder",
            "refanswer.status",
            "refanswer.updatedate",
            "refanswer.updateuser"
        ])
    }} AS ingestion_sk,
    refanswer.answergroupid,
    refanswer.answerid,
    refanswer.answermodulename,
    refanswer.answertitle,
    refanswer.buttontype,
    refanswer.createdate,
    refanswer.createuser,
    refanswer.description,
    refanswer.orgid,
    refanswer.sortorder,
    refanswer.status,
    refanswer.updatedate,
    refanswer.updateuser

FROM
    {{ source("bay_dbo", "refanswer") }} AS refanswer
