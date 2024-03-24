{{-
    config(
        materialized = "table",
        tags = ["execute_daily", "area", "super_table"]
    )
-}}


SELECT
    divisions.division_id_int,
    divisions.division_name,

    district.district_id_hash,
    districts.district_id_int,
    district.district_name,
    districts.district_lat,
    districts.district_lon,

    upazilas.upazila_id_int,
    upazilas.upazila_name,

    unions.union_id_int,
    unions.union_name

FROM
    {{ ref("bse_dbo_district") }} AS district

    INNER JOIN {{ ref("bse_dbo_districts") }} AS districts
        ON
            district.district_name = IF(
                districts.district_name = "COXSBAZAR",
                "COX'S BAZAR",
                districts.district_name
            )

    LEFT OUTER JOIN {{ ref("bse_dbo_divisions") }} AS divisions
        ON
            districts.division_id_int = divisions.division_id_int

    LEFT OUTER JOIN {{ ref("bse_dbo_upazilas") }} AS upazilas
        ON
            districts.district_id_int = upazilas.district_id_int

    LEFT OUTER JOIN {{ ref("bse_dbo_unions") }} AS unions
        ON
            upazilas.upazila_id_int = unions.upazila_id_int
