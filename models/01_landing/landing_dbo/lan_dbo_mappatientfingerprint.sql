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
            "`mappatientfingerprint`.`createdate`",
            "`mappatientfingerprint`.`createuser`",
            "`mappatientfingerprint`.`fingerprintid`",
            "`mappatientfingerprint`.`mappingpatientfingerprintid`",
            "`mappatientfingerprint`.`orgid`",
            "`mappatientfingerprint`.`patientid`",
            "`mappatientfingerprint`.`status`",
            "`mappatientfingerprint`.`updatedate`",
            "`mappatientfingerprint`.`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `mappatientfingerprint`.`createdate`,
    `mappatientfingerprint`.`createuser`,
    `mappatientfingerprint`.`fingerprintid`,
    `mappatientfingerprint`.`mappingpatientfingerprintid`,
    `mappatientfingerprint`.`orgid`,
    `mappatientfingerprint`.`patientid`,
    `mappatientfingerprint`.`status`,
    `mappatientfingerprint`.`updatedate`,
    `mappatientfingerprint`.`updateuser`

FROM
    {{ source("bay_dbo", "mappatientfingerprint") }} AS `mappatientfingerprint`
