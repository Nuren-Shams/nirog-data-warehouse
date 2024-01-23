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
            "refprovisionaldiagnosisgroup.category",
            "refprovisionaldiagnosisgroup.commonterm",
            "refprovisionaldiagnosisgroup.createdate",
            "refprovisionaldiagnosisgroup.createuser",
            "refprovisionaldiagnosisgroup.orgid",
            "refprovisionaldiagnosisgroup.refprovisionaldiagnosisgroupcode",
            "refprovisionaldiagnosisgroup.refprovisionaldiagnosisgroupid",
            "refprovisionaldiagnosisgroup.sortorder",
            "refprovisionaldiagnosisgroup.status",
            "refprovisionaldiagnosisgroup.updatedate",
            "refprovisionaldiagnosisgroup.updateuser"
        ])
    }} AS ingestion_sk,
    refprovisionaldiagnosisgroup.category,
    refprovisionaldiagnosisgroup.commonterm,
    refprovisionaldiagnosisgroup.createdate,
    refprovisionaldiagnosisgroup.createuser,
    refprovisionaldiagnosisgroup.orgid,
    refprovisionaldiagnosisgroup.refprovisionaldiagnosisgroupcode,
    refprovisionaldiagnosisgroup.refprovisionaldiagnosisgroupid,
    refprovisionaldiagnosisgroup.sortorder,
    refprovisionaldiagnosisgroup.status,
    refprovisionaldiagnosisgroup.updatedate,
    refprovisionaldiagnosisgroup.updateuser

FROM
    {{ source("bay_dbo", "refprovisionaldiagnosisgroup") }} AS refprovisionaldiagnosisgroup
