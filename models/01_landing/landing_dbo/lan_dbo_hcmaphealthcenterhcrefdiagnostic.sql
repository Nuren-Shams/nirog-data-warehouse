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
            "hcmaphealthcenterhcrefdiagnostic.createdate",
            "hcmaphealthcenterhcrefdiagnostic.createuser",
            "hcmaphealthcenterhcrefdiagnostic.hcdiagnosticfee",
            "hcmaphealthcenterhcrefdiagnostic.hcrefdiagnosticid",
            "hcmaphealthcenterhcrefdiagnostic.healthcenterid",
            "hcmaphealthcenterhcrefdiagnostic.isprovideservice",
            "hcmaphealthcenterhcrefdiagnostic.mappinghealthcenterhcrefdiagnosticid",
            "hcmaphealthcenterhcrefdiagnostic.orgid",
            "hcmaphealthcenterhcrefdiagnostic.status",
            "hcmaphealthcenterhcrefdiagnostic.updatedate",
            "hcmaphealthcenterhcrefdiagnostic.updateuser"
        ])
    }} AS ingestion_sk,
    hcmaphealthcenterhcrefdiagnostic.createdate,
    hcmaphealthcenterhcrefdiagnostic.createuser,
    hcmaphealthcenterhcrefdiagnostic.hcdiagnosticfee,
    hcmaphealthcenterhcrefdiagnostic.hcrefdiagnosticid,
    hcmaphealthcenterhcrefdiagnostic.healthcenterid,
    hcmaphealthcenterhcrefdiagnostic.isprovideservice,
    hcmaphealthcenterhcrefdiagnostic.mappinghealthcenterhcrefdiagnosticid,
    hcmaphealthcenterhcrefdiagnostic.orgid,
    hcmaphealthcenterhcrefdiagnostic.status,
    hcmaphealthcenterhcrefdiagnostic.updatedate,
    hcmaphealthcenterhcrefdiagnostic.updateuser

FROM
    {{ source("bay_dbo", "hcmaphealthcenterhcrefdiagnostic") }} AS hcmaphealthcenterhcrefdiagnostic
