# Overview

This is the Nirog Plus Data Warehouse code repository. It utilizes the DBT framework to build and maintain Nirog's data warehouse on GCP BigQuery.

## Setup

Perform a `git clone` operation to clone the repository to a local/cloud compute instance. Place the Production (mandatory) and the Development (optional) GCP auto-generated credential files inside the `./credentials/` folder. Then change the `keyfile` values for the `production` (mandatory) and `development` (optional) targets inside the `./core/profiles/profiles.yml` file to reflect the names of the `*.json` GCP auto-generated credential files.

## Initialize

From a CMD Terminal, run the `init.bat` script. This initializer routine will take care of the one-off execution environment initialization tasks including virtual-environment creation, dependency installations, connection testing etc. This operation needs to be performed once per deployment.

## Run

From a CMD Terminal run the `run.bat` script. This runner routine expects the execution environment to be preemptively setup (refer to the *Setup* section) and initialized (refer to the *Initialize* section). This runner script is pre-configured to run the production target for DBT and performs the entire end-to-end ETL operation.
