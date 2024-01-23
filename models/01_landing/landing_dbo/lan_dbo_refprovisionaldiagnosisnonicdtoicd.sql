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
            "refprovisionaldiagnosisnonicdtoicd.icdcodefordiagnosis1",
            "refprovisionaldiagnosisnonicdtoicd.icdcodefordiagnosis2",
            "refprovisionaldiagnosisnonicdtoicd.icdcodefordiagnosis3",
            "refprovisionaldiagnosisnonicdtoicd.icdcodefordiagnosis4",
            "refprovisionaldiagnosisnonicdtoicd.icdcodefordiagnosis5",
            "refprovisionaldiagnosisnonicdtoicd.provisionaldiagnosiscode"
        ])
    }} AS ingestion_sk,
    refprovisionaldiagnosisnonicdtoicd.icdcodefordiagnosis1,
    refprovisionaldiagnosisnonicdtoicd.icdcodefordiagnosis2,
    refprovisionaldiagnosisnonicdtoicd.icdcodefordiagnosis3,
    refprovisionaldiagnosisnonicdtoicd.icdcodefordiagnosis4,
    refprovisionaldiagnosisnonicdtoicd.icdcodefordiagnosis5,
    refprovisionaldiagnosisnonicdtoicd.provisionaldiagnosiscode

FROM
    {{ source("bay_dbo", "refprovisionaldiagnosisnonicdtoicd") }} AS refprovisionaldiagnosisnonicdtoicd
