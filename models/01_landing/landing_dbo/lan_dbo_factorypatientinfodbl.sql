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
            "factorypatientinfodbl.age",
            "factorypatientinfodbl.age_0_1_years",
            "factorypatientinfodbl.age_1_5_years",
            "factorypatientinfodbl.age_over_5_years",
            "factorypatientinfodbl.birth_date",
            "factorypatientinfodbl.company_name",
            "factorypatientinfodbl.country",
            "factorypatientinfodbl.department",
            "factorypatientinfodbl.designation",
            "factorypatientinfodbl.district",
            "factorypatientinfodbl.education",
            "factorypatientinfodbl.emailaddress",
            "factorypatientinfodbl.employee_id",
            "factorypatientinfodbl.family_name",
            "factorypatientinfodbl.father_or_husband_name",
            "factorypatientinfodbl.gender",
            "factorypatientinfodbl.given_name",
            "factorypatientinfodbl.head_of_family",
            "factorypatientinfodbl.house_no",
            "factorypatientinfodbl.joiningdate",
            "factorypatientinfodbl.marital_status",
            "factorypatientinfodbl.mobile_no",
            "factorypatientinfodbl.mother_name",
            "factorypatientinfodbl.national_id_no",
            "factorypatientinfodbl.no_of_family_member",
            "factorypatientinfodbl.religion",
            "factorypatientinfodbl.road_no",
            "factorypatientinfodbl.thana",
            "factorypatientinfodbl.village"
        ])
    }} AS ingestion_sk,
    factorypatientinfodbl.age,
    factorypatientinfodbl.age_0_1_years,
    factorypatientinfodbl.age_1_5_years,
    factorypatientinfodbl.age_over_5_years,
    factorypatientinfodbl.birth_date,
    factorypatientinfodbl.company_name,
    factorypatientinfodbl.country,
    factorypatientinfodbl.department,
    factorypatientinfodbl.designation,
    factorypatientinfodbl.district,
    factorypatientinfodbl.education,
    factorypatientinfodbl.emailaddress,
    factorypatientinfodbl.employee_id,
    factorypatientinfodbl.family_name,
    factorypatientinfodbl.father_or_husband_name,
    factorypatientinfodbl.gender,
    factorypatientinfodbl.given_name,
    factorypatientinfodbl.head_of_family,
    factorypatientinfodbl.house_no,
    factorypatientinfodbl.joiningdate,
    factorypatientinfodbl.marital_status,
    factorypatientinfodbl.mobile_no,
    factorypatientinfodbl.mother_name,
    factorypatientinfodbl.national_id_no,
    factorypatientinfodbl.no_of_family_member,
    factorypatientinfodbl.religion,
    factorypatientinfodbl.road_no,
    factorypatientinfodbl.thana,
    factorypatientinfodbl.village

FROM
    {{ source("bay_dbo", "factorypatientinfodbl") }} AS factorypatientinfodbl
