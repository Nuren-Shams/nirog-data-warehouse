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
            "mdatapatientquestionanswer.answerid",
            "mdatapatientquestionanswer.collectiondate",
            "mdatapatientquestionanswer.comment",
            "mdatapatientquestionanswer.createdate",
            "mdatapatientquestionanswer.createuser",
            "mdatapatientquestionanswer.mdpatientquestionanswerid",
            "mdatapatientquestionanswer.orgid",
            "mdatapatientquestionanswer.patientid",
            "mdatapatientquestionanswer.questionid",
            "mdatapatientquestionanswer.status",
            "mdatapatientquestionanswer.updatedate",
            "mdatapatientquestionanswer.updateuser"
        ])
    }} AS ingestion_sk,
    mdatapatientquestionanswer.answerid,
    mdatapatientquestionanswer.collectiondate,
    mdatapatientquestionanswer.comment,
    mdatapatientquestionanswer.createdate,
    mdatapatientquestionanswer.createuser,
    mdatapatientquestionanswer.mdpatientquestionanswerid,
    mdatapatientquestionanswer.orgid,
    mdatapatientquestionanswer.patientid,
    mdatapatientquestionanswer.questionid,
    mdatapatientquestionanswer.status,
    mdatapatientquestionanswer.updatedate,
    mdatapatientquestionanswer.updateuser

FROM
    {{ source("bay_dbo", "mdatapatientquestionanswer") }} AS mdatapatientquestionanswer
