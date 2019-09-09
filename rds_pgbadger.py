import datetime
import os
from itertools import groupby

import boto3
import click
import luigi
from luigi.contrib.external_program import ExternalProgramTask
from luigi.contrib.s3 import S3Target

from utils import extract_date_from_log_file_name, hash_list
from rds_download_log import get_database_region, get_credentials, get_log_file_via_rest

class PgBadgerReportFileToS3(luigi.Task):
    s3_bucket = luigi.Parameter()
    db_instance_identifier = luigi.Parameter()
    file_name = luigi.Parameter()

    def requires(self):
        return PgBadgerReportFile(
            db_instance_identifier=self.db_instance_identifier,
            file_name=self.file_name,
        )

    def output(self):
        return S3Target(f's3://{self.s3_bucket}/{self.db_instance_identifier}/{os.path.basename(self.file_name)}.html')

    def run(self):
        with self.output().open('w') as fo:
            with self.input().open('r') as fi:
                fo.write(fi.read())


class PgBadgerReportFile(ExternalProgramTask):
    db_instance_identifier = luigi.Parameter()
    file_name = luigi.Parameter()

    def requires(self):
        return DBLogFile(
            db_instance_identifier=self.db_instance_identifier,
            file_name=self.file_name,
        )

    def output(self):
        return luigi.LocalTarget(f'reports/{self.db_instance_identifier}/{self.file_name}.html')

    def run(self):
        with self.output().temporary_path() as self.temp_output_path:
            return super().run()

    def program_args(self):
        return [
            'pgbadger',
            '--jobs',
            '8',
            '--prefix',
            '%t:%r:%u@%d:[%p]:',
            '--format',
            'stderr',
            '--outfile',
            self.temp_output_path,
            self.input().path,
        ]


class DBLogFile(luigi.Task):
    db_instance_identifier = luigi.Parameter()
    file_name = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(f'data/{self.db_instance_identifier}/{self.file_name}')

    def run(self):
        with self.output().open('w') as out_file:
            out_file.write(get_log_file_via_rest(self.file_name, self.db_instance_identifier))

class MainTask(luigi.Task):
    s3_bucket = luigi.Parameter()
    db_instance_identifier = luigi.Parameter()
    max_records = luigi.IntParameter(default=5)
    reference_date_hour = luigi.Parameter(default=datetime.datetime.utcnow().strftime('%Y-%m-%d-%H'))

    def requires(self):
        client = boto3.client('rds')

        def collect_for_day(reference):
            # Collect DB logs from RDS that contain the date string for the given reference date
            response = client.describe_db_log_files(
                DBInstanceIdentifier=self.db_instance_identifier,
                FilenameContains=reference.strftime('%Y-%m-%d'),
                MaxRecords=24,
            )
            return [log_file['LogFileName'] for log_file in response['DescribeDBLogFiles']]

        resolved = set()
        log_file_names = list()
        last_reference = datetime.datetime.strptime(self.reference_date_hour, '%Y-%m-%d-%H')
        for i in range(self.max_records // 24 or 1):
            # In chunks of 24 hours, collect all the available log file names
            log_file_names.extend(collect_for_day(last_reference))
            resolved.add(last_reference.date())
            last_reference -= datetime.timedelta(days=1)

        last_reference = datetime.datetime.strptime(self.reference_date_hour, '%Y-%m-%d-%H')
        last_reference -= datetime.timedelta(hours=self.max_records)
        if last_reference.date() not in resolved:
            # In case we are close to midnight (i.e. 2AM) and we are only collecting a few records (i.e. the default 5),
            # check if the hours delta is on a different day, and collect those if needed
            log_file_names.extend(collect_for_day(last_reference))
            resolved.add(last_reference.date())

        # Skip current hour as entries could still be written to the log file.
        log_file_names = [
            log_file_name for log_file_name in log_file_names if not log_file_name.endswith(self.reference_date_hour)
        ]

        # Sort by date in descending order
        log_file_names = sorted(
            log_file_names,
            key=lambda log_file_name: datetime.datetime.strptime(log_file_name[-13:], '%Y-%m-%d-%H'),
            reverse=True,
        )

        for log_file_name in log_file_names[:self.max_records]:
            yield PgBadgerReportFileToS3(
                s3_bucket=self.s3_bucket,
                db_instance_identifier=self.db_instance_identifier,
                file_name=log_file_name,
            )


@click.command()
@click.option('--target-s3-bucket', required=True, envvar='TARGET_S3_BUCKET')
@click.option('--database-instance-identifier', required=True, envvar='DATABASE_INSTANCE_IDENTIFIER')
@click.option('--reference-datetime', required=False, type=click.DateTime())
def main(target_s3_bucket, database_instance_identifier, reference_datetime):
    params = dict(
        s3_bucket=target_s3_bucket,
        db_instance_identifier=database_instance_identifier,
    )
    if reference_datetime:
        params['reference_date_hour'] = reference_datetime.strftime('%Y-%m-%d-%H')

    luigi.build(
        [
            MainTask(**params),
        ],
        local_scheduler=True,
    )


if __name__ == '__main__':
    main()
