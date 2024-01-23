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
            "factorypatientprothomalo.age",
            "factorypatientprothomalo.age_0_1_years",
            "factorypatientprothomalo.age_1_5_years",
            "factorypatientprothomalo.age_over_5_years",
            "factorypatientprothomalo.birth_date",
            "factorypatientprothomalo.company_name",
            "factorypatientprothomalo.country",
            "factorypatientprothomalo.department",
            "factorypatientprothomalo.designation",
            "factorypatientprothomalo.district",
            "factorypatientprothomalo.education",
            "factorypatientprothomalo.emailaddress",
            "factorypatientprothomalo.employee_id",
            "factorypatientprothomalo.family_name",
            "factorypatientprothomalo.father_or_husband_name",
            "factorypatientprothomalo.gender",
            "factorypatientprothomalo.given_name",
            "factorypatientprothomalo.head_of_family",
            "factorypatientprothomalo.house_no",
            "factorypatientprothomalo.joiningdate",
            "factorypatientprothomalo.marital_status",
            "factorypatientprothomalo.mobile_no",
            "factorypatientprothomalo.mother_name",
            "factorypatientprothomalo.national_id_no",
            "factorypatientprothomalo.no_of_family_member",
            "factorypatientprothomalo.religion",
            "factorypatientprothomalo.road_no",
            "factorypatientprothomalo.thana",
            "factorypatientprothomalo.village"
        ])
    }} AS ingestion_sk,
    factorypatientprothomalo.age,
    factorypatientprothomalo.age_0_1_years,
    factorypatientprothomalo.age_1_5_years,
    factorypatientprothomalo.age_over_5_years,
    factorypatientprothomalo.birth_date,
    factorypatientprothomalo.company_name,
    factorypatientprothomalo.country,
    factorypatientprothomalo.department,
    factorypatientprothomalo.designation,
    factorypatientprothomalo.district,
    factorypatientprothomalo.education,
    factorypatientprothomalo.emailaddress,
    factorypatientprothomalo.employee_id,
    factorypatientprothomalo.family_name,
    factorypatientprothomalo.father_or_husband_name,
    factorypatientprothomalo.gender,
    factorypatientprothomalo.given_name,
    factorypatientprothomalo.head_of_family,
    factorypatientprothomalo.house_no,
    factorypatientprothomalo.joiningdate,
    factorypatientprothomalo.marital_status,
    factorypatientprothomalo.mobile_no,
    factorypatientprothomalo.mother_name,
    factorypatientprothomalo.national_id_no,
    factorypatientprothomalo.no_of_family_member,
    factorypatientprothomalo.religion,
    factorypatientprothomalo.road_no,
    factorypatientprothomalo.thana,
    factorypatientprothomalo.village

FROM
    {{ source("bay_dbo", "factorypatientprothomalo") }} AS factorypatientprothomalo
