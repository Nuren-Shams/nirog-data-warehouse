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
            "refquestiontype.createdate",
            "refquestiontype.createuser",
            "refquestiontype.orgid",
            "refquestiontype.questiontypecode",
            "refquestiontype.questiontypeid",
            "refquestiontype.questiontypetitle",
            "refquestiontype.sortorder",
            "refquestiontype.status",
            "refquestiontype.updatedate",
            "refquestiontype.updateuser"
        ])
    }} AS ingestion_sk,
    refquestiontype.createdate,
    refquestiontype.createuser,
    refquestiontype.orgid,
    refquestiontype.questiontypecode,
    refquestiontype.questiontypeid,
    refquestiontype.questiontypetitle,
    refquestiontype.sortorder,
    refquestiontype.status,
    refquestiontype.updatedate,
    refquestiontype.updateuser

FROM
    {{ source("bay_dbo", "refquestiontype") }} AS refquestiontype
