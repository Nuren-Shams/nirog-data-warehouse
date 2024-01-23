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
            "users.avatar",
            "users.cc_id",
            "users.created_at",
            "users.created_by",
            "users.email",
            "users.email_verified_at",
            "users.employeeid",
            "users.gender",
            "users.id",
            "users.mobile_no",
            "users.modified_by",
            "users.name",
            "users.orgid",
            "users.password",
            "users.remember_token",
            "users.role_id",
            "users.station",
            "users.status",
            "users.updated_at"
        ])
    }} AS ingestion_sk,
    users.avatar,
    users.cc_id,
    users.created_at,
    users.created_by,
    users.email,
    users.email_verified_at,
    users.employeeid,
    users.gender,
    users.id,
    users.mobile_no,
    users.modified_by,
    users.name,
    users.orgid,
    users.password,
    users.remember_token,
    users.role_id,
    users.station,
    users.status,
    users.updated_at

FROM
    {{ source("bay_dbo", "users") }} AS users
