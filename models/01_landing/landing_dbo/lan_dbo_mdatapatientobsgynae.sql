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
            "`childmortality0to1`",
            "`childmortalitybelow5`",
            "`childmortalityover5`",
            "`collectiondate`",
            "`comment`",
            "`contraceptionmethodid`",
            "`createdate`",
            "`createuser`",
            "`gravida`",
            "`iscervicalcancer`",
            "`ispregnant`",
            "`isreuse`",
            "`livingbirth`",
            "`livingfemale`",
            "`livingmale`",
            "`lmp`",
            "`mdpatientobsgynaeid`",
            "`menstruationproductid`",
            "`menstruationproductusagetimeid`",
            "`miscarraigeorabortion`",
            "`mr`",
            "`orgid`",
            "`othercontraceptionmethod`",
            "`othermenstruationproduct`",
            "`othermenstruationproductusagetime`",
            "`para`",
            "`patientid`",
            "`status`",
            "`stillbirth`",
            "`updatedate`",
            "`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `childmortality0to1`,
    `childmortalitybelow5`,
    `childmortalityover5`,
    `collectiondate`,
    `comment`,
    `contraceptionmethodid`,
    `createdate`,
    `createuser`,
    `gravida`,
    `iscervicalcancer`,
    `ispregnant`,
    `isreuse`,
    `livingbirth`,
    `livingfemale`,
    `livingmale`,
    `lmp`,
    `mdpatientobsgynaeid`,
    `menstruationproductid`,
    `menstruationproductusagetimeid`,
    `miscarraigeorabortion`,
    `mr`,
    `orgid`,
    `othercontraceptionmethod`,
    `othermenstruationproduct`,
    `othermenstruationproductusagetime`,
    `para`,
    `patientid`,
    `status`,
    `stillbirth`,
    `updatedate`,
    `updateuser`

FROM
    {{ source("bay_dbo", "mdatapatientobsgynae") }}
