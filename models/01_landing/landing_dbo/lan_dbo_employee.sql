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
            "`birthdate`",
            "`createdate`",
            "`createuser`",
            "`designation`",
            "`educationid`",
            "`email`",
            "`employeecode`",
            "`employeeid`",
            "`employeeimage`",
            "`employeesignature`",
            "`firstname`",
            "`genderid`",
            "`joiningdate`",
            "`lastname`",
            "`maritalstatusid`",
            "`nationalidnumber`",
            "`orgid`",
            "`phone`",
            "`registrationnumber`",
            "`religionid`",
            "`roleid`",
            "`status`",
            "`updatedate`",
            "`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `birthdate`,
    `createdate`,
    `createuser`,
    `designation`,
    `educationid`,
    `email`,
    `employeecode`,
    `employeeid`,
    `employeeimage`,
    `employeesignature`,
    `firstname`,
    `genderid`,
    `joiningdate`,
    `lastname`,
    `maritalstatusid`,
    `nationalidnumber`,
    `orgid`,
    `phone`,
    `registrationnumber`,
    `religionid`,
    `roleid`,
    `status`,
    `updatedate`,
    `updateuser`

FROM
    {{ source("bay_dbo", "employee") }}
