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
            "icdcodefordiagnosis1",
            "icdcodefordiagnosis2",
            "icdcodefordiagnosis3",
            "icdcodefordiagnosis4",
            "icdcodefordiagnosis5",
            "provisionaldiagnosiscode"
        ])
    }} AS ingestion_sk,
    icdcodefordiagnosis1,
    icdcodefordiagnosis2,
    icdcodefordiagnosis3,
    icdcodefordiagnosis4,
    icdcodefordiagnosis5,
    provisionaldiagnosiscode

FROM
    {{ source("bay_dbo", "refprovisionaldiagnosisnonicdtoicd") }}
