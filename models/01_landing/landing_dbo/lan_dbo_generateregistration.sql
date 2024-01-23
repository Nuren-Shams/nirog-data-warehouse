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
            "generateregistration.createdate",
            "generateregistration.createuserid",
            "generateregistration.endid",
            "generateregistration.generateregistrationid",
            "generateregistration.numberofid",
            "generateregistration.startid",
            "generateregistration.updatedate",
            "generateregistration.updateuserid",
            "generateregistration.workplacebranchid"
        ])
    }} AS ingestion_sk,
    generateregistration.createdate,
    generateregistration.createuserid,
    generateregistration.endid,
    generateregistration.generateregistrationid,
    generateregistration.numberofid,
    generateregistration.startid,
    generateregistration.updatedate,
    generateregistration.updateuserid,
    generateregistration.workplacebranchid

FROM
    {{ source("bay_dbo", "generateregistration") }} AS generateregistration
