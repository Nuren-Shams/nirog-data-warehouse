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
            "refquestion.createdate",
            "refquestion.createuser",
            "refquestion.description",
            "refquestion.orgid",
            "refquestion.questiongroupid",
            "refquestion.questionid",
            "refquestion.questionmodulename",
            "refquestion.questiontitle",
            "refquestion.questiontypeid",
            "refquestion.sortorder",
            "refquestion.status",
            "refquestion.updatedate",
            "refquestion.updateuser"
        ])
    }} AS ingestion_sk,
    refquestion.createdate,
    refquestion.createuser,
    refquestion.description,
    refquestion.orgid,
    refquestion.questiongroupid,
    refquestion.questionid,
    refquestion.questionmodulename,
    refquestion.questiontitle,
    refquestion.questiontypeid,
    refquestion.sortorder,
    refquestion.status,
    refquestion.updatedate,
    refquestion.updateuser

FROM
    {{ source("bay_dbo", "refquestion") }} AS refquestion
