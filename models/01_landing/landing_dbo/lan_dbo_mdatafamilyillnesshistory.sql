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
            "createdate",
            "createuser",
            "deceasedyears",
            "illfamilymemberid",
            "illnessid",
            "mdfamilyillnessid",
            "orgid",
            "otherillfamilymember",
            "otherillness",
            "patientid",
            "status",
            "updatedate",
            "updateuser"
        ])
    }} AS ingestion_sk,
    collectiondate,
    createdate,
    createuser,
    deceasedyears,
    illfamilymemberid,
    illnessid,
    mdfamilyillnessid,
    orgid,
    otherillfamilymember,
    otherillness,
    patientid,
    status,
    updatedate,
    updateuser

FROM
    {{ source("bay_dbo", "mdatafamilyillnesshistory") }}
