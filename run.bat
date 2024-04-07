CALL conda activate nirog-data-warehouse-environment
cd %~dp0\core
dbt build --profiles-dir .\profiles --target production
CALL conda deactivate
PAUSE
