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
            "refinstruction.createdate",
            "refinstruction.createuser",
            "refinstruction.description",
            "refinstruction.instructioncode",
            "refinstruction.instructioninbangla",
            "refinstruction.instructioninenglish",
            "refinstruction.orgid",
            "refinstruction.refinstructionid",
            "refinstruction.sortorder",
            "refinstruction.status",
            "refinstruction.updatedate",
            "refinstruction.updateuser"
        ])
    }} AS ingestion_sk,
    refinstruction.createdate,
    refinstruction.createuser,
    refinstruction.description,
    refinstruction.instructioncode,
    refinstruction.instructioninbangla,
    refinstruction.instructioninenglish,
    refinstruction.orgid,
    refinstruction.refinstructionid,
    refinstruction.sortorder,
    refinstruction.status,
    refinstruction.updatedate,
    refinstruction.updateuser

FROM
    {{ source("bay_dbo", "refinstruction") }} AS refinstruction
