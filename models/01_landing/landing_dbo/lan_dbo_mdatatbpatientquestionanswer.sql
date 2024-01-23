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
            "mdatatbpatientquestionanswer.answerid",
            "mdatatbpatientquestionanswer.collectiondate",
            "mdatatbpatientquestionanswer.createdate",
            "mdatatbpatientquestionanswer.createuser",
            "mdatatbpatientquestionanswer.mdtbpatientquestionanswerid",
            "mdatatbpatientquestionanswer.orgid",
            "mdatatbpatientquestionanswer.otheranswer",
            "mdatatbpatientquestionanswer.patientid",
            "mdatatbpatientquestionanswer.questionid",
            "mdatatbpatientquestionanswer.status",
            "mdatatbpatientquestionanswer.updatedate",
            "mdatatbpatientquestionanswer.updateuser"
        ])
    }} AS ingestion_sk,
    mdatatbpatientquestionanswer.answerid,
    mdatatbpatientquestionanswer.collectiondate,
    mdatatbpatientquestionanswer.createdate,
    mdatatbpatientquestionanswer.createuser,
    mdatatbpatientquestionanswer.mdtbpatientquestionanswerid,
    mdatatbpatientquestionanswer.orgid,
    mdatatbpatientquestionanswer.otheranswer,
    mdatatbpatientquestionanswer.patientid,
    mdatatbpatientquestionanswer.questionid,
    mdatatbpatientquestionanswer.status,
    mdatatbpatientquestionanswer.updatedate,
    mdatatbpatientquestionanswer.updateuser

FROM
    {{ source("bay_dbo", "mdatatbpatientquestionanswer") }} AS mdatatbpatientquestionanswer
