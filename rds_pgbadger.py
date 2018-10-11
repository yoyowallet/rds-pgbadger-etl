import datetime
import os
from itertools import groupby

import boto3
import click
import luigi
from luigi.contrib.external_program import ExternalProgramTask
from luigi.contrib.s3 import S3Target

from utils import extract_date_from_log_file_name, hash_list


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
        client = boto3.client('rds')

        with self.output().open('w') as out_file:
            marker = '0'
            while True:
                print(f'Marker: {marker}')
                response = client.download_db_log_file_portion(
                    DBInstanceIdentifier=self.db_instance_identifier,
                    LogFileName=self.file_name,
                    Marker=marker,
                    NumberOfLines=10000,
                )
                marker = response['Marker']
                out_file.write(response['LogFileData'])
                if not response['AdditionalDataPending']:
                    break


class MainTask(luigi.Task):
    s3_bucket = luigi.Parameter()
    db_instance_identifier = luigi.Parameter()
    max_records = luigi.IntParameter(default=5)

    def requires(self):
        client = boto3.client('rds')
        response = client.describe_db_log_files(
            DBInstanceIdentifier=self.db_instance_identifier,
            MaxRecords=self.max_records,
        )

        date_now = datetime.datetime.utcnow().strftime('%Y-%m-%d-%H')

        for log_file in response['DescribeDBLogFiles']:
            if log_file['LogFileName'].endswith(date_now):
                # Skip current hour as entries could still be written to the log file.
                continue

            yield PgBadgerReportFileToS3(
                s3_bucket=self.s3_bucket,
                db_instance_identifier=self.db_instance_identifier,
                file_name=log_file['LogFileName'],
            )


@click.command()
@click.option('--target-s3-bucket', required=True, envvar='TARGET_S3_BUCKET')
@click.option('--database-instance-identifier', required=True, envvar='DATABASE_INSTANCE_IDENTIFIER')
def main(target_s3_bucket, database_instance_identifier):
    luigi.build(
        [
            MainTask(
                s3_bucket=target_s3_bucket,
                db_instance_identifier=database_instance_identifier,
            ),
        ],
        local_scheduler=True,
    )


if __name__ == '__main__':
    main()
