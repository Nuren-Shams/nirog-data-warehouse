CALL conda clean --all --verbose --yes
CALL conda env remove --name nirog-data-warehouse-environment --verbose --yes
CALL conda create --name nirog-data-warehouse-environment python=3.10.14 --verbose --yes
CALL conda activate nirog-data-warehouse-environment
python -m pip install pip --upgrade --no-cache-dir --verbose
pip install -r %~dp0\requirements.txt --no-cache-dir --verbose
cd %~dp0\core
dbt deps
dbt debug --profiles-dir .\profiles
CALL conda deactivate
PAUSE
