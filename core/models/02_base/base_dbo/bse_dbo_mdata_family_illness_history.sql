{{-
    config(
        materialized = "table",
        tags = ["execute_daily"]
    )
-}}

SELECT
    IF(UPPER(illfamilymemberid) IN ("NONE", ""), NULL, UPPER(illfamilymemberid)) AS ill_family_member_id,
    IF(UPPER(illnessid) IN ("NONE", ""), NULL, UPPER(illnessid)) AS illness_id,
    IF(UPPER(mdfamilyillnessid) IN ("NONE", ""), NULL, UPPER(mdfamilyillnessid)) AS md_family_illness_id,
    IF(UPPER(patientid) IN ("NONE", ""), NULL, UPPER(patientid)) AS patient_id,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", collectiondate) AS collected_at,
    DATE(SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", collectiondate)) AS collected_date,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", createdate) AS created_at,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", updatedate) AS updated_at,
    IF(UPPER(otherillfamilymember) IN ("NONE", ""), NULL, UPPER(otherillfamilymember)) AS other_ill_family_member,
    IF(UPPER(otherillness) IN ("NONE", ""), NULL, UPPER(otherillness)) AS other_illness,
    IF(UPPER(status) IN ("NONE", ""), NULL, UPPER(status)) AS illness_status,

FROM
    {{ ref("lan_dbo_mdatafamilyillnesshistory") }}

QUALIFY ROW_NUMBER() OVER(PARTITION BY patient_id, collected_date, other_ill_family_member, illness_id ORDER BY updated_at DESC) = 1
