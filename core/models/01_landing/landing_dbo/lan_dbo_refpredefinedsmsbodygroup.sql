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
            "`refpredefinedsmsbodygroup`.`createdate`",
            "`refpredefinedsmsbodygroup`.`createuser`",
            "`refpredefinedsmsbodygroup`.`description`",
            "`refpredefinedsmsbodygroup`.`orgid`",
            "`refpredefinedsmsbodygroup`.`refpredefinedsmsbodygroupcode`",
            "`refpredefinedsmsbodygroup`.`refpredefinedsmsbodygroupid`",
            "`refpredefinedsmsbodygroup`.`sortorder`",
            "`refpredefinedsmsbodygroup`.`status`",
            "`refpredefinedsmsbodygroup`.`updatedate`",
            "`refpredefinedsmsbodygroup`.`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `refpredefinedsmsbodygroup`.`createdate`,
    `refpredefinedsmsbodygroup`.`createuser`,
    `refpredefinedsmsbodygroup`.`description`,
    `refpredefinedsmsbodygroup`.`orgid`,
    `refpredefinedsmsbodygroup`.`refpredefinedsmsbodygroupcode`,
    `refpredefinedsmsbodygroup`.`refpredefinedsmsbodygroupid`,
    `refpredefinedsmsbodygroup`.`sortorder`,
    `refpredefinedsmsbodygroup`.`status`,
    `refpredefinedsmsbodygroup`.`updatedate`,
    `refpredefinedsmsbodygroup`.`updateuser`

FROM
    {{ source("bay_dbo", "refpredefinedsmsbodygroup") }} AS `refpredefinedsmsbodygroup`
