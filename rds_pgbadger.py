from pprint import pprint

import boto3
import luigi
from luigi.contrib.external_program import ExternalProgramTask


class PgBadgerReportFile(ExternalProgramTask):
    db_name = luigi.Parameter()
    file_name = luigi.Parameter()

    def requires(self):
        return DBLogFile(
            db_name=self.db_name,
            file_name=self.file_name,
        )

    def output(self):
        return luigi.LocalTarget(f'reports/{self.db_name}/{self.file_name}.html')

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
                response = client.download_db_log_file_portion(
                    DBInstanceIdentifier=self.db_name,
                    LogFileName=self.file_name,
                    Marker=marker,
                )
                marker = response['Marker']
                out_file.write(response['LogFileData'])
                if not response['AdditionalDataPending']:
                    break


class MainTask(luigi.Task):
    db_name = luigi.Parameter()

    def requires(self):
        client = boto3.client('rds')
        response = client.describe_db_log_files(
            DBInstanceIdentifier=self.db_name,
        )
        for log_file in response['DescribeDBLogFiles']:
            yield PgBadgerReportFile(
                db_name=self.db_name,
                file_name=log_file['LogFileName'],
            )
