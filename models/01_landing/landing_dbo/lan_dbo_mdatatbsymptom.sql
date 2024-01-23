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
            "mdatatbsymptom.collectiondate",
            "mdatatbsymptom.createdate",
            "mdatatbsymptom.createuser",
            "mdatatbsymptom.mdtbsymptomid",
            "mdatatbsymptom.orgid",
            "mdatatbsymptom.otherssymptom",
            "mdatatbsymptom.patientid",
            "mdatatbsymptom.status",
            "mdatatbsymptom.tbsymptom",
            "mdatatbsymptom.tbsymptom1",
            "mdatatbsymptom.tbsymptom2",
            "mdatatbsymptom.tbsymptom3",
            "mdatatbsymptom.tbsymptom4",
            "mdatatbsymptom.tbsymptom5",
            "mdatatbsymptom.tbsymptom6",
            "mdatatbsymptom.tbsymptom7",
            "mdatatbsymptom.tbsymptom8",
            "mdatatbsymptom.tbsymptomcode",
            "mdatatbsymptom.tbsymptomcode1",
            "mdatatbsymptom.tbsymptomcode2",
            "mdatatbsymptom.tbsymptomcode3",
            "mdatatbsymptom.tbsymptomcode4",
            "mdatatbsymptom.tbsymptomcode5",
            "mdatatbsymptom.tbsymptomcode6",
            "mdatatbsymptom.tbsymptomcode7",
            "mdatatbsymptom.tbsymptomcode8",
            "mdatatbsymptom.updatedate",
            "mdatatbsymptom.updateuser"
        ])
    }} AS ingestion_sk,
    mdatatbsymptom.collectiondate,
    mdatatbsymptom.createdate,
    mdatatbsymptom.createuser,
    mdatatbsymptom.mdtbsymptomid,
    mdatatbsymptom.orgid,
    mdatatbsymptom.otherssymptom,
    mdatatbsymptom.patientid,
    mdatatbsymptom.status,
    mdatatbsymptom.tbsymptom,
    mdatatbsymptom.tbsymptom1,
    mdatatbsymptom.tbsymptom2,
    mdatatbsymptom.tbsymptom3,
    mdatatbsymptom.tbsymptom4,
    mdatatbsymptom.tbsymptom5,
    mdatatbsymptom.tbsymptom6,
    mdatatbsymptom.tbsymptom7,
    mdatatbsymptom.tbsymptom8,
    mdatatbsymptom.tbsymptomcode,
    mdatatbsymptom.tbsymptomcode1,
    mdatatbsymptom.tbsymptomcode2,
    mdatatbsymptom.tbsymptomcode3,
    mdatatbsymptom.tbsymptomcode4,
    mdatatbsymptom.tbsymptomcode5,
    mdatatbsymptom.tbsymptomcode6,
    mdatatbsymptom.tbsymptomcode7,
    mdatatbsymptom.tbsymptomcode8,
    mdatatbsymptom.updatedate,
    mdatatbsymptom.updateuser

FROM
    {{ source("bay_dbo", "mdatatbsymptom") }} AS mdatatbsymptom
