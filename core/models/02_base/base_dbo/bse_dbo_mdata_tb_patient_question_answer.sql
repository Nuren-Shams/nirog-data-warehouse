{{-
    config(
        materialized = "table",
        tags = ["execute_daily"]
    )
-}}

SELECT
    IF(UPPER(mdtbpatientquestionanswerid) IN ("NONE", ""), NULL, UPPER(mdtbpatientquestionanswerid)) AS md_tb_patient_question_answer_id,
    IF(UPPER(questionid) IN ("NONE", ""), NULL, UPPER(questionid)) AS question_id,
    IF(UPPER(answerid) IN ("NONE", ""), NULL, UPPER(answerid)) AS answer_id,
    IF(UPPER(patientid) IN ("NONE", ""), NULL, UPPER(patientid)) AS patient_id,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", collectiondate) AS collected_at,
    DATE(SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", collectiondate)) AS collected_date,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", createdate) AS created_at,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", updatedate) AS updated_at

FROM
    {{ ref("lan_dbo_mdatatbpatientquestionanswer") }}

QUALIFY ROW_NUMBER() OVER (PARTITION BY patient_id, collected_date, question_id, answer_id ORDER BY updated_at DESC) = 1
