# Data Warehouse
Data tranformation for Validus 

## Models structure

```
models/
├── standardized
├── conformed
├── enriched
```
* `standardized`: The refined data (type casting, basic transformations, etc.)
* `conformed`: Insightful data used for reporting and analytics
* `enriched`: Same as conformed, aggregated at higher granularity
 
## Getting started
### 1. Clone project
---

### 2. Prepare environment variables
Prepare `.env` file including at least below environment variables under `redshift` folder.

```sh
REDSHIFT_HOST="server.region.redshift-serverless.amazonaws.com:5439/dev"
REDSHIFT_USER="username"
REDSHIFT_PASSWORD="password"
REDSHIFT_DBNAME="dev"
DBT_PROFILES_DIR="profile/"
```

### 3. Install required packages

```
dbt-redshift
dotenv-cli
```

### 4. Test connection
```
cd redshift/
dotenv dbt debug
```

## Naming convention
* Prefix:
    * Tables under standardized will have the source as the prefix. e.g: `sf_` for Salesforce, `wca_` for WCA Database
    * Tables under conformed will have `cfm_` as the prefix
    * Tables under enriched will have `enr_` as the prefix
    
* Suffix:
    * `_t` for table
    * `_v` for view
    * `_mv` for materialized view

### Incremental strategy
* Salesforce:
    * Using the macro `generate_salesforce_incr_condition` for incremental models 

### Scheduling
Using airflow for scheduling
TBU
