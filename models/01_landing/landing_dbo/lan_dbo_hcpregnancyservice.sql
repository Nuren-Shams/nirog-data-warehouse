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
            "hcpregnancyservice.antenatalfee",
            "hcpregnancyservice.cesareandeliveryfee",
            "hcpregnancyservice.createdate",
            "hcpregnancyservice.createuser",
            "hcpregnancyservice.emergencydeliveryfee",
            "hcpregnancyservice.epideliveryfee",
            "hcpregnancyservice.hcpregnancyserviceid",
            "hcpregnancyservice.healthcenterid",
            "hcpregnancyservice.homedeliveryfee",
            "hcpregnancyservice.isprovideantenatal",
            "hcpregnancyservice.isprovidecesareandelivery",
            "hcpregnancyservice.isprovideemergencydeliveryoptions",
            "hcpregnancyservice.isprovideepidelivery",
            "hcpregnancyservice.isprovidehomedelivery",
            "hcpregnancyservice.isprovidematernalimmunization",
            "hcpregnancyservice.isprovidenormaldelivery",
            "hcpregnancyservice.isprovidepregnancyservice",
            "hcpregnancyservice.maternalimmunizationfee",
            "hcpregnancyservice.normaldeliveryfee",
            "hcpregnancyservice.orgid",
            "hcpregnancyservice.status",
            "hcpregnancyservice.updatedate",
            "hcpregnancyservice.updateuser"
        ])
    }} AS ingestion_sk,
    hcpregnancyservice.antenatalfee,
    hcpregnancyservice.cesareandeliveryfee,
    hcpregnancyservice.createdate,
    hcpregnancyservice.createuser,
    hcpregnancyservice.emergencydeliveryfee,
    hcpregnancyservice.epideliveryfee,
    hcpregnancyservice.hcpregnancyserviceid,
    hcpregnancyservice.healthcenterid,
    hcpregnancyservice.homedeliveryfee,
    hcpregnancyservice.isprovideantenatal,
    hcpregnancyservice.isprovidecesareandelivery,
    hcpregnancyservice.isprovideemergencydeliveryoptions,
    hcpregnancyservice.isprovideepidelivery,
    hcpregnancyservice.isprovidehomedelivery,
    hcpregnancyservice.isprovidematernalimmunization,
    hcpregnancyservice.isprovidenormaldelivery,
    hcpregnancyservice.isprovidepregnancyservice,
    hcpregnancyservice.maternalimmunizationfee,
    hcpregnancyservice.normaldeliveryfee,
    hcpregnancyservice.orgid,
    hcpregnancyservice.status,
    hcpregnancyservice.updatedate,
    hcpregnancyservice.updateuser

FROM
    {{ source("bay_dbo", "hcpregnancyservice") }} AS hcpregnancyservice
