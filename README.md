# rds-pgbadger-etl
ETL pipeline for generating pgBadger reports from RDS instances

## Requirements

- Python 3.6+
- `pipenv`

## Installation

```bash
pipenv install
```

## Usage

### Using local scheduler
You first need to install [pgbadger](https://github.com/darold/pgbadger).

For instance, in Linux simply run `apt-get install pgbadger`.

Then you can execute the script as:

```bash
$ AWS_ACCESS_KEY_ID="your_access_key" AWS_SECRET_ACCESS_KEY="your_secret_access_key" AWS_DEFAULT_REGION="db_region" python rds_pgbadger.py --target-s3-bucket "name_of_the_bucket" --database-instance-identifier "db_name" --reference-datetime "2019-04-01T10:00:00"
```
Example:
```bash
$ AWS_ACCESS_KEY_ID="your_access_key" AWS_SECRET_ACCESS_KEY="your_secret_access_key" AWS_DEFAULT_REGION="eu-west-1" python rds_pgbadger.py --target-s3-bucket "yoyowallet-prod2-rds-pgbadger-reports" --database-instance-identifier "prod2-v4-readreplica-1" --reference-datetime "2019-04-01T10:00:00"
```
