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
            "refcat.catid",
            "refcat.catmedicinestatus",
            "refcat.cattype",
            "refcat.collectiondate",
            "refcat.comment1",
            "refcat.comment2",
            "refcat.createdate",
            "refcat.createuser",
            "refcat.drugdose",
            "refcat.drugdurationvalue",
            "refcat.drugid",
            "refcat.frequency",
            "refcat.hour",
            "refcat.orgid",
            "refcat.otherdrug",
            "refcat.specialinstruction",
            "refcat.status",
            "refcat.updatedate",
            "refcat.updateuser"
        ])
    }} AS ingestion_sk,
    refcat.catid,
    refcat.catmedicinestatus,
    refcat.cattype,
    refcat.collectiondate,
    refcat.comment1,
    refcat.comment2,
    refcat.createdate,
    refcat.createuser,
    refcat.drugdose,
    refcat.drugdurationvalue,
    refcat.drugid,
    refcat.frequency,
    refcat.hour,
    refcat.orgid,
    refcat.otherdrug,
    refcat.specialinstruction,
    refcat.status,
    refcat.updatedate,
    refcat.updateuser

FROM
    {{ source("bay_dbo", "refcat") }} AS refcat
