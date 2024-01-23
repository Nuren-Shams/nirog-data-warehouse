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
            "`hcbloodbankfacility`.`bloodgroupingfee`",
            "`hcbloodbankfacility`.`bloodtransfusionfee`",
            "`hcbloodbankfacility`.`createdate`",
            "`hcbloodbankfacility`.`createuser`",
            "`hcbloodbankfacility`.`crossmatchingfee`",
            "`hcbloodbankfacility`.`freshfrozenplasmafee`",
            "`hcbloodbankfacility`.`hcbloodbankfacilityid`",
            "`hcbloodbankfacility`.`healthcenterid`",
            "`hcbloodbankfacility`.`isaffiliationwithquantum`",
            "`hcbloodbankfacility`.`isaffiliationwithsandhani`",
            "`hcbloodbankfacility`.`isprovidebloodgrouping`",
            "`hcbloodbankfacility`.`isprovidebloodtransfusion`",
            "`hcbloodbankfacility`.`isprovidecrossmatching`",
            "`hcbloodbankfacility`.`isprovidefreshfrozenplasma`",
            "`hcbloodbankfacility`.`isprovideplateletconcentrate`",
            "`hcbloodbankfacility`.`orgid`",
            "`hcbloodbankfacility`.`plateletconcentratefee`",
            "`hcbloodbankfacility`.`status`",
            "`hcbloodbankfacility`.`updatedate`",
            "`hcbloodbankfacility`.`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `hcbloodbankfacility`.`bloodgroupingfee`,
    `hcbloodbankfacility`.`bloodtransfusionfee`,
    `hcbloodbankfacility`.`createdate`,
    `hcbloodbankfacility`.`createuser`,
    `hcbloodbankfacility`.`crossmatchingfee`,
    `hcbloodbankfacility`.`freshfrozenplasmafee`,
    `hcbloodbankfacility`.`hcbloodbankfacilityid`,
    `hcbloodbankfacility`.`healthcenterid`,
    `hcbloodbankfacility`.`isaffiliationwithquantum`,
    `hcbloodbankfacility`.`isaffiliationwithsandhani`,
    `hcbloodbankfacility`.`isprovidebloodgrouping`,
    `hcbloodbankfacility`.`isprovidebloodtransfusion`,
    `hcbloodbankfacility`.`isprovidecrossmatching`,
    `hcbloodbankfacility`.`isprovidefreshfrozenplasma`,
    `hcbloodbankfacility`.`isprovideplateletconcentrate`,
    `hcbloodbankfacility`.`orgid`,
    `hcbloodbankfacility`.`plateletconcentratefee`,
    `hcbloodbankfacility`.`status`,
    `hcbloodbankfacility`.`updatedate`,
    `hcbloodbankfacility`.`updateuser`

FROM
    {{ source("bay_dbo", "hcbloodbankfacility") }} AS `hcbloodbankfacility`
