# rds-pgbadger-etl
ETL pipeline for generating pgBadger reports from RDS instances

## Requirements

- Python 3.6+

## Installation

```bash
$ make deps
```

## Usage

Using local scheduler:
```bash
$ python -m luigi --module rds_pgbadger MainTask --db-name prod2-v4 --local-scheduler
```
