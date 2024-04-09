{{-
    config(
        materialized = "table",
        tags = ["execute_daily"]
    )
-}}

SELECT
    IF(UPPER(patientid) IN ("NONE", ""), NULL, UPPER(patientid)) AS patient_id,
    IF(UPPER(mdphysicalexamgeneralid) IN ("NONE", ""), NULL, UPPER(mdphysicalexamgeneralid)) AS md_physical_exam_general_id,
    IF(UPPER(orgid) IN ("NONE", ""), NULL, UPPER(orgid)) AS org_id,

    SAFE_CAST(SAFE_CAST(anemiaseverity AS FLOAT64) AS INT64) AS anemia_severity,
    SAFE_CAST(SAFE_CAST(edemaseverity AS FLOAT64) AS INT64) AS edema_severity,
    SAFE_CAST(SAFE_CAST(jaundiceseverity AS FLOAT64) AS INT64) AS jaundice_severity,
    SAFE_CAST(cyanosis AS INT64) AS cyanosis,
    IF(UPPER(isheartwithnad) IN ("NONE", ""), NULL, UPPER(isheartwithnad)) AS is_heart_with_nad,
    IF(UPPER(islungswithnad) IN ("NONE", ""), NULL, UPPER(islungswithnad)) AS is_lungs_with_nad,
    IF(UPPER(islymphnodeswithpalpable) IN ("NONE", ""), NULL, UPPER(islymphnodeswithpalpable)) AS is_lymph_nodes_with_palpable,

    IF(UPPER(heartwithnad) IN ("NONE", ""), NULL, UPPER(heartwithnad)) AS heart_with_nad,
    IF(UPPER(lungswithnad) IN ("NONE", ""), NULL, UPPER(lungswithnad)) AS lungs_with_nad,
    IF(UPPER(lymphnodeswithpalpablesite) IN ("NONE", ""), NULL, UPPER(lymphnodeswithpalpablesite)) AS lymph_nodes_with_palpable_site,
    IF(UPPER(lymphnodeswithpalpablesize) IN ("NONE", ""), NULL, UPPER(lymphnodeswithpalpablesize)) AS lymph_nodes_with_palpable_size,
    

    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", collectiondate) AS collected_at,
    DATE(SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", collectiondate)) AS collected_date,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", createdate) AS created_at,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", updatedate) AS updated_at

FROM
    {{ ref("lan_dbo_mdataphysicalexamgeneral") }}

QUALIFY ROW_NUMBER() OVER (PARTITION BY patient_id, collected_date ORDER BY updated_at DESC) = 1
