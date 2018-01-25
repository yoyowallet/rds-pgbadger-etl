from itertools import groupby

import boto3
import luigi
from luigi.contrib.external_program import ExternalProgramTask

from utils import extract_date_from_log_file_name, hash_list


class PgBadgerReportFile(ExternalProgramTask):
    db_name = luigi.Parameter()
    date = luigi.Parameter()
    file_names = luigi.ListParameter()

    def requires(self):
        return ConsolidatedDBLogFile(
            db_name=self.db_name,
            date=self.date,
            file_names=self.file_names,
        )

    def output(self):
        return luigi.LocalTarget(f'reports/{self.db_name}/{self.date}-{hash_list(self.file_names)}.html')

    def run(self):
        self.output().makedirs()
        with self.output().temporary_path() as self.temp_output_path:
            return super().run()

    def program_args(self):
        return [
            'pgbadger',
            '--jobs',
            '8',
            '--prefix',
            '%t:%r:%u@%d:[%p]:',
            '--outfile',
            self.temp_output_path,
            self.input().path,
        ]


class DBLogFile(luigi.Task):
    db_name = luigi.Parameter()
    file_name = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(f'data/{self.db_name}/{self.file_name}')

    def run(self):
        client = boto3.client('rds')

        with self.output().open('w') as out_file:
            marker = '0'
            while True:
                print(f'Marker: {marker}')
                response = client.download_db_log_file_portion(
                    DBInstanceIdentifier=self.db_name,
                    LogFileName=self.file_name,
                    Marker=marker,
                    NumberOfLines=10000,
                )
                marker = response['Marker']
                out_file.write(response['LogFileData'])
                if not response['AdditionalDataPending']:
                    break


class ConsolidatedDBLogFile(luigi.Task):
    db_name = luigi.Parameter()
    date = luigi.Parameter()
    file_names = luigi.ListParameter()

    def requires(self):
        return [
            DBLogFile(
                db_name=self.db_name,
                file_name=file_name,
            )
            for file_name in self.file_names
        ]

    def output(self):
        return luigi.LocalTarget(f'data/{self.db_name}/consolidated/{self.date}-{hash_list(self.file_names)}.log')

    def run(self):
        with self.output().open('w') as out_file:
            for log_file in self.input():
                with log_file.open() as in_file:
                    out_file.write(in_file.read())


class MainTask(luigi.Task):
    db_name = luigi.Parameter()

    def requires(self):
        client = boto3.client('rds')
        response = client.describe_db_log_files(
            DBInstanceIdentifier=self.db_name,
        )
        all_logs = [
            log_file['LogFileName']
            for log_file in response['DescribeDBLogFiles']
        ]
        logs_grouped_by_date = groupby(all_logs, key=extract_date_from_log_file_name)
        for log_date, log_files in logs_grouped_by_date:
            yield PgBadgerReportFile(
                db_name=self.db_name,
                date=log_date,
                file_names=list(log_files),
            )
