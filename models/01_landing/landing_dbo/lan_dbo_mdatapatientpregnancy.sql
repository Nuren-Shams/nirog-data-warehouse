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
            "collectiondate",
            "comment",
            "createdate",
            "createuser",
            "edd",
            "foetalheartrate",
            "foetalheartsound",
            "foetalmovement",
            "foetalposition",
            "lmp",
            "mdpatientpregnancyid",
            "orgid",
            "otherfoetalpositioninfo",
            "patientid",
            "pregnancydurationinweeks",
            "status",
            "symphysiofundalheight",
            "updatedate",
            "updateuser",
            "usg"
        ])
    }} AS ingestion_sk,
    collectiondate,
    comment,
    createdate,
    createuser,
    edd,
    foetalheartrate,
    foetalheartsound,
    foetalmovement,
    foetalposition,
    lmp,
    mdpatientpregnancyid,
    orgid,
    otherfoetalpositioninfo,
    patientid,
    pregnancydurationinweeks,
    status,
    symphysiofundalheight,
    updatedate,
    updateuser,
    usg

FROM
    {{ source("bay_dbo", "mdatapatientpregnancy") }}
