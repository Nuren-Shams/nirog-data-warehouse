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
            "`logchild`.`imactedrows`",
            "`logchild`.`logdetailmessage`",
            "`logchild`.`logdetailsid`",
            "`logchild`.`logtime`",
            "`logchild`.`optype`",
            "`logchild`.`procdetailname`",
            "`logchild`.`proclogid`",
            "`logchild`.`tablename`"
        ])
    }} AS `ingestion_sk`,
    `logchild`.`imactedrows`,
    `logchild`.`logdetailmessage`,
    `logchild`.`logdetailsid`,
    `logchild`.`logtime`,
    `logchild`.`optype`,
    `logchild`.`procdetailname`,
    `logchild`.`proclogid`,
    `logchild`.`tablename`

FROM
    {{ source("bay_dbo", "logchild") }} AS `logchild`
