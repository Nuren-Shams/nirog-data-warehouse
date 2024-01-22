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
            "antenatalfee",
            "cesareandeliveryfee",
            "createdate",
            "createuser",
            "emergencydeliveryfee",
            "epideliveryfee",
            "hcpregnancyserviceid",
            "healthcenterid",
            "homedeliveryfee",
            "isprovideantenatal",
            "isprovidecesareandelivery",
            "isprovideemergencydeliveryoptions",
            "isprovideepidelivery",
            "isprovidehomedelivery",
            "isprovidematernalimmunization",
            "isprovidenormaldelivery",
            "isprovidepregnancyservice",
            "maternalimmunizationfee",
            "normaldeliveryfee",
            "orgid",
            "status",
            "updatedate",
            "updateuser"
        ])
    }} AS ingestion_sk,
    antenatalfee,
    cesareandeliveryfee,
    createdate,
    createuser,
    emergencydeliveryfee,
    epideliveryfee,
    hcpregnancyserviceid,
    healthcenterid,
    homedeliveryfee,
    isprovideantenatal,
    isprovidecesareandelivery,
    isprovideemergencydeliveryoptions,
    isprovideepidelivery,
    isprovidehomedelivery,
    isprovidematernalimmunization,
    isprovidenormaldelivery,
    isprovidepregnancyservice,
    maternalimmunizationfee,
    normaldeliveryfee,
    orgid,
    status,
    updatedate,
    updateuser

FROM
    {{ source("bay_dbo", "hcpregnancyservice") }}
