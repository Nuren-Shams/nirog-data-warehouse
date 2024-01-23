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
            "mapworkplacedepartmentdesignation.createdate",
            "mapworkplacedepartmentdesignation.createuser",
            "mapworkplacedepartmentdesignation.mappingworkplacedepartmentdesignationid",
            "mapworkplacedepartmentdesignation.orgid",
            "mapworkplacedepartmentdesignation.refdepartmentid",
            "mapworkplacedepartmentdesignation.refdesignationid",
            "mapworkplacedepartmentdesignation.status",
            "mapworkplacedepartmentdesignation.updatedate",
            "mapworkplacedepartmentdesignation.updateuser"
        ])
    }} AS ingestion_sk,
    mapworkplacedepartmentdesignation.createdate,
    mapworkplacedepartmentdesignation.createuser,
    mapworkplacedepartmentdesignation.mappingworkplacedepartmentdesignationid,
    mapworkplacedepartmentdesignation.orgid,
    mapworkplacedepartmentdesignation.refdepartmentid,
    mapworkplacedepartmentdesignation.refdesignationid,
    mapworkplacedepartmentdesignation.status,
    mapworkplacedepartmentdesignation.updatedate,
    mapworkplacedepartmentdesignation.updateuser

FROM
    {{ source("bay_dbo", "mapworkplacedepartmentdesignation") }} AS mapworkplacedepartmentdesignation
