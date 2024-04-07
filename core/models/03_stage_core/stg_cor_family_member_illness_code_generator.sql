{{-
    config(
        materialized = "table",
        tags = ["execute_daily", "mdata", "family_member_illness_code", "generator"]
    )
-}}

WITH family_member_illness_code_spine AS (
    SELECT DISTINCT
        LOWER(
            CONCAT(
                other_ill_family_member, "_", ri.illness_code
            )
        ) AS family_member_illness_code

    FROM
        {{ ref("bse_dbo_mdata_family_illness_history") }} AS fih

    CROSS JOIN
        UNNEST(
            SPLIT(
                COALESCE(fih.other_ill_family_member, "OTHER"), ","
            )
        ) AS other_ill_family_member

    CROSS JOIN (
        SELECT DISTINCT illness_code
        FROM {{ ref("bse_dbo_ref_illness") }}
    ) AS ri

    ORDER BY
        family_member_illness_code ASC
)

SELECT
    STRING_AGG(CONCAT("is_", family_member_illness_code), ",\n" ORDER BY family_member_illness_code ASC) AS family_member_illness_code_id,
    STRING_AGG(CONCAT('"', family_member_illness_code, '"'), ",\n" ORDER BY family_member_illness_code ASC) AS family_member_illness_code_value

FROM
    family_member_illness_code_spine
