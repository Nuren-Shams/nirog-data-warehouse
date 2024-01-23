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
            "employee.birthdate",
            "employee.createdate",
            "employee.createuser",
            "employee.designation",
            "employee.educationid",
            "employee.email",
            "employee.employeecode",
            "employee.employeeid",
            "employee.employeeimage",
            "employee.employeesignature",
            "employee.firstname",
            "employee.genderid",
            "employee.joiningdate",
            "employee.lastname",
            "employee.maritalstatusid",
            "employee.nationalidnumber",
            "employee.orgid",
            "employee.phone",
            "employee.registrationnumber",
            "employee.religionid",
            "employee.roleid",
            "employee.status",
            "employee.updatedate",
            "employee.updateuser"
        ])
    }} AS ingestion_sk,
    employee.birthdate,
    employee.createdate,
    employee.createuser,
    employee.designation,
    employee.educationid,
    employee.email,
    employee.employeecode,
    employee.employeeid,
    employee.employeeimage,
    employee.employeesignature,
    employee.firstname,
    employee.genderid,
    employee.joiningdate,
    employee.lastname,
    employee.maritalstatusid,
    employee.nationalidnumber,
    employee.orgid,
    employee.phone,
    employee.registrationnumber,
    employee.religionid,
    employee.roleid,
    employee.status,
    employee.updatedate,
    employee.updateuser

FROM
    {{ source("bay_dbo", "employee") }} AS employee
