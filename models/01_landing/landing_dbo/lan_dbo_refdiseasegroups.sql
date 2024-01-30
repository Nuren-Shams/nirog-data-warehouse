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
            "`refdiseasegroups`.`createdate`",
            "`refdiseasegroups`.`createuser`",
            "`refdiseasegroups`.`diseasegroupname`",
            "`refdiseasegroups`.`orgid`",
            "`refdiseasegroups`.`refdiseasegroupid`",
            "`refdiseasegroups`.`sortorder`",
            "`refdiseasegroups`.`status`",
            "`refdiseasegroups`.`updatedate`",
            "`refdiseasegroups`.`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `refdiseasegroups`.`createdate`,
    `refdiseasegroups`.`createuser`,
    `refdiseasegroups`.`diseasegroupname`,
    `refdiseasegroups`.`orgid`,
    `refdiseasegroups`.`refdiseasegroupid`,
    `refdiseasegroups`.`sortorder`,
    `refdiseasegroups`.`status`,
    `refdiseasegroups`.`updatedate`,
    `refdiseasegroups`.`updateuser`

FROM
    {{ source("bay_dbo", "refdiseasegroups") }} AS `refdiseasegroups`
