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
            "`refmenstruationproduct`.`createdate`",
            "`refmenstruationproduct`.`createuser`",
            "`refmenstruationproduct`.`description`",
            "`refmenstruationproduct`.`menstruationproductcode`",
            "`refmenstruationproduct`.`menstruationproductid`",
            "`refmenstruationproduct`.`orgid`",
            "`refmenstruationproduct`.`sortorder`",
            "`refmenstruationproduct`.`status`",
            "`refmenstruationproduct`.`updatedate`",
            "`refmenstruationproduct`.`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `refmenstruationproduct`.`createdate`,
    `refmenstruationproduct`.`createuser`,
    `refmenstruationproduct`.`description`,
    `refmenstruationproduct`.`menstruationproductcode`,
    `refmenstruationproduct`.`menstruationproductid`,
    `refmenstruationproduct`.`orgid`,
    `refmenstruationproduct`.`sortorder`,
    `refmenstruationproduct`.`status`,
    `refmenstruationproduct`.`updatedate`,
    `refmenstruationproduct`.`updateuser`

FROM
    {{ source("bay_dbo", "refmenstruationproduct") }} AS `refmenstruationproduct`
