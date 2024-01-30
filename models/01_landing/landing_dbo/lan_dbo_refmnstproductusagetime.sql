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
            "`refmnstproductusagetime`.`createdate`",
            "`refmnstproductusagetime`.`createuser`",
            "`refmnstproductusagetime`.`description`",
            "`refmnstproductusagetime`.`menstruationproductusagetimecode`",
            "`refmnstproductusagetime`.`menstruationproductusagetimeid`",
            "`refmnstproductusagetime`.`orgid`",
            "`refmnstproductusagetime`.`sortorder`",
            "`refmnstproductusagetime`.`status`",
            "`refmnstproductusagetime`.`updatedate`",
            "`refmnstproductusagetime`.`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `refmnstproductusagetime`.`createdate`,
    `refmnstproductusagetime`.`createuser`,
    `refmnstproductusagetime`.`description`,
    `refmnstproductusagetime`.`menstruationproductusagetimecode`,
    `refmnstproductusagetime`.`menstruationproductusagetimeid`,
    `refmnstproductusagetime`.`orgid`,
    `refmnstproductusagetime`.`sortorder`,
    `refmnstproductusagetime`.`status`,
    `refmnstproductusagetime`.`updatedate`,
    `refmnstproductusagetime`.`updateuser`

FROM
    {{ source("bay_dbo", "refmnstproductusagetime") }} AS `refmnstproductusagetime`
