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
            "refpredefinedsmsbody.banglastatus",
            "refpredefinedsmsbody.createdate",
            "refpredefinedsmsbody.createuser",
            "refpredefinedsmsbody.description",
            "refpredefinedsmsbody.orgid",
            "refpredefinedsmsbody.predefinedsmsbody",
            "refpredefinedsmsbody.refpredefinedsmsbodygroupid",
            "refpredefinedsmsbody.refpredefinedsmsbodyid",
            "refpredefinedsmsbody.sortorder",
            "refpredefinedsmsbody.status",
            "refpredefinedsmsbody.updatedate",
            "refpredefinedsmsbody.updateuser"
        ])
    }} AS ingestion_sk,
    refpredefinedsmsbody.banglastatus,
    refpredefinedsmsbody.createdate,
    refpredefinedsmsbody.createuser,
    refpredefinedsmsbody.description,
    refpredefinedsmsbody.orgid,
    refpredefinedsmsbody.predefinedsmsbody,
    refpredefinedsmsbody.refpredefinedsmsbodygroupid,
    refpredefinedsmsbody.refpredefinedsmsbodyid,
    refpredefinedsmsbody.sortorder,
    refpredefinedsmsbody.status,
    refpredefinedsmsbody.updatedate,
    refpredefinedsmsbody.updateuser

FROM
    {{ source("bay_dbo", "refpredefinedsmsbody") }} AS refpredefinedsmsbody
