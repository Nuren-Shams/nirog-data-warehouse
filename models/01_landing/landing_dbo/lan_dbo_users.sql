{{
    config(
        materialized = "incremental",
        unique_key = "ingestion_sk",
        tags = ["execute_daily"]
    )
}}


SELECT
    {{
        dbt_utils.generate_surrogate_key([
            "avatar",
            "cc_id",
            "created_at",
            "created_by",
            "email",
            "email_verified_at",
            "employeeid",
            "gender",
            "id",
            "mobile_no",
            "modified_by",
            "name",
            "orgid",
            "password",
            "remember_token",
            "role_id",
            "station",
            "status",
            "updated_at"
        ])
    }} AS ingestion_sk,
    avatar,
    cc_id,
    created_at,
    created_by,
    email,
    email_verified_at,
    employeeid,
    gender,
    id,
    mobile_no,
    modified_by,
    name,
    orgid,
    password,
    remember_token,
    role_id,
    station,
    status,
    updated_at

FROM
    {{ source("bay_dbo", "users") }}
