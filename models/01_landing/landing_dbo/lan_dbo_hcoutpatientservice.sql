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
            "`createdate`",
            "`createuser`",
            "`generaloutpaientfee`",
            "`hcrefoutpatientserviceid`",
            "`healthcenterid`",
            "`isprovideoutpatientservice`",
            "`numberoflabtechnicians`",
            "`numberofnurses`",
            "`numberofphysicians`",
            "`numberofsupportstaffs`",
            "`numberofultrasoundtechnicians`",
            "`numberofxraytechnicians`",
            "`orgid`",
            "`outpatientserviceendtime`",
            "`outpatientservicestarttime`",
            "`status`",
            "`updatedate`",
            "`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `createdate`,
    `createuser`,
    `generaloutpaientfee`,
    `hcrefoutpatientserviceid`,
    `healthcenterid`,
    `isprovideoutpatientservice`,
    `numberoflabtechnicians`,
    `numberofnurses`,
    `numberofphysicians`,
    `numberofsupportstaffs`,
    `numberofultrasoundtechnicians`,
    `numberofxraytechnicians`,
    `orgid`,
    `outpatientserviceendtime`,
    `outpatientservicestarttime`,
    `status`,
    `updatedate`,
    `updateuser`

FROM
    {{ source("bay_dbo", "hcoutpatientservice") }}
