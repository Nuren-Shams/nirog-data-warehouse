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
            "`bloodgroupingfee`",
            "`bloodtransfusionfee`",
            "`createdate`",
            "`createuser`",
            "`crossmatchingfee`",
            "`freshfrozenplasmafee`",
            "`hcbloodbankfacilityid`",
            "`healthcenterid`",
            "`isaffiliationwithquantum`",
            "`isaffiliationwithsandhani`",
            "`isprovidebloodgrouping`",
            "`isprovidebloodtransfusion`",
            "`isprovidecrossmatching`",
            "`isprovidefreshfrozenplasma`",
            "`isprovideplateletconcentrate`",
            "`orgid`",
            "`plateletconcentratefee`",
            "`status`",
            "`updatedate`",
            "`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `bloodgroupingfee`,
    `bloodtransfusionfee`,
    `createdate`,
    `createuser`,
    `crossmatchingfee`,
    `freshfrozenplasmafee`,
    `hcbloodbankfacilityid`,
    `healthcenterid`,
    `isaffiliationwithquantum`,
    `isaffiliationwithsandhani`,
    `isprovidebloodgrouping`,
    `isprovidebloodtransfusion`,
    `isprovidecrossmatching`,
    `isprovidefreshfrozenplasma`,
    `isprovideplateletconcentrate`,
    `orgid`,
    `plateletconcentratefee`,
    `status`,
    `updatedate`,
    `updateuser`

FROM
    {{ source("bay_dbo", "hcbloodbankfacility") }}
