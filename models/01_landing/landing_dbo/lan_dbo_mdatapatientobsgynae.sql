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
            "mdatapatientobsgynae.childmortality0to1",
            "mdatapatientobsgynae.childmortalitybelow5",
            "mdatapatientobsgynae.childmortalityover5",
            "mdatapatientobsgynae.collectiondate",
            "mdatapatientobsgynae.comment",
            "mdatapatientobsgynae.contraceptionmethodid",
            "mdatapatientobsgynae.createdate",
            "mdatapatientobsgynae.createuser",
            "mdatapatientobsgynae.gravida",
            "mdatapatientobsgynae.iscervicalcancer",
            "mdatapatientobsgynae.ispregnant",
            "mdatapatientobsgynae.isreuse",
            "mdatapatientobsgynae.livingbirth",
            "mdatapatientobsgynae.livingfemale",
            "mdatapatientobsgynae.livingmale",
            "mdatapatientobsgynae.lmp",
            "mdatapatientobsgynae.mdpatientobsgynaeid",
            "mdatapatientobsgynae.menstruationproductid",
            "mdatapatientobsgynae.menstruationproductusagetimeid",
            "mdatapatientobsgynae.miscarraigeorabortion",
            "mdatapatientobsgynae.mr",
            "mdatapatientobsgynae.orgid",
            "mdatapatientobsgynae.othercontraceptionmethod",
            "mdatapatientobsgynae.othermenstruationproduct",
            "mdatapatientobsgynae.othermenstruationproductusagetime",
            "mdatapatientobsgynae.para",
            "mdatapatientobsgynae.patientid",
            "mdatapatientobsgynae.status",
            "mdatapatientobsgynae.stillbirth",
            "mdatapatientobsgynae.updatedate",
            "mdatapatientobsgynae.updateuser"
        ])
    }} AS ingestion_sk,
    mdatapatientobsgynae.childmortality0to1,
    mdatapatientobsgynae.childmortalitybelow5,
    mdatapatientobsgynae.childmortalityover5,
    mdatapatientobsgynae.collectiondate,
    mdatapatientobsgynae.comment,
    mdatapatientobsgynae.contraceptionmethodid,
    mdatapatientobsgynae.createdate,
    mdatapatientobsgynae.createuser,
    mdatapatientobsgynae.gravida,
    mdatapatientobsgynae.iscervicalcancer,
    mdatapatientobsgynae.ispregnant,
    mdatapatientobsgynae.isreuse,
    mdatapatientobsgynae.livingbirth,
    mdatapatientobsgynae.livingfemale,
    mdatapatientobsgynae.livingmale,
    mdatapatientobsgynae.lmp,
    mdatapatientobsgynae.mdpatientobsgynaeid,
    mdatapatientobsgynae.menstruationproductid,
    mdatapatientobsgynae.menstruationproductusagetimeid,
    mdatapatientobsgynae.miscarraigeorabortion,
    mdatapatientobsgynae.mr,
    mdatapatientobsgynae.orgid,
    mdatapatientobsgynae.othercontraceptionmethod,
    mdatapatientobsgynae.othermenstruationproduct,
    mdatapatientobsgynae.othermenstruationproductusagetime,
    mdatapatientobsgynae.para,
    mdatapatientobsgynae.patientid,
    mdatapatientobsgynae.status,
    mdatapatientobsgynae.stillbirth,
    mdatapatientobsgynae.updatedate,
    mdatapatientobsgynae.updateuser

FROM
    {{ source("bay_dbo", "mdatapatientobsgynae") }} AS mdatapatientobsgynae
