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
            "`hcoutpatientservice`.`createdate`",
            "`hcoutpatientservice`.`createuser`",
            "`hcoutpatientservice`.`generaloutpaientfee`",
            "`hcoutpatientservice`.`hcrefoutpatientserviceid`",
            "`hcoutpatientservice`.`healthcenterid`",
            "`hcoutpatientservice`.`isprovideoutpatientservice`",
            "`hcoutpatientservice`.`numberoflabtechnicians`",
            "`hcoutpatientservice`.`numberofnurses`",
            "`hcoutpatientservice`.`numberofphysicians`",
            "`hcoutpatientservice`.`numberofsupportstaffs`",
            "`hcoutpatientservice`.`numberofultrasoundtechnicians`",
            "`hcoutpatientservice`.`numberofxraytechnicians`",
            "`hcoutpatientservice`.`orgid`",
            "`hcoutpatientservice`.`outpatientserviceendtime`",
            "`hcoutpatientservice`.`outpatientservicestarttime`",
            "`hcoutpatientservice`.`status`",
            "`hcoutpatientservice`.`updatedate`",
            "`hcoutpatientservice`.`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `hcoutpatientservice`.`createdate`,
    `hcoutpatientservice`.`createuser`,
    `hcoutpatientservice`.`generaloutpaientfee`,
    `hcoutpatientservice`.`hcrefoutpatientserviceid`,
    `hcoutpatientservice`.`healthcenterid`,
    `hcoutpatientservice`.`isprovideoutpatientservice`,
    `hcoutpatientservice`.`numberoflabtechnicians`,
    `hcoutpatientservice`.`numberofnurses`,
    `hcoutpatientservice`.`numberofphysicians`,
    `hcoutpatientservice`.`numberofsupportstaffs`,
    `hcoutpatientservice`.`numberofultrasoundtechnicians`,
    `hcoutpatientservice`.`numberofxraytechnicians`,
    `hcoutpatientservice`.`orgid`,
    `hcoutpatientservice`.`outpatientserviceendtime`,
    `hcoutpatientservice`.`outpatientservicestarttime`,
    `hcoutpatientservice`.`status`,
    `hcoutpatientservice`.`updatedate`,
    `hcoutpatientservice`.`updateuser`

FROM
    {{ source("bay_dbo", "hcoutpatientservice") }} AS `hcoutpatientservice`
