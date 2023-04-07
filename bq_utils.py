from google.cloud import bigquery
import json
import glob
import os


class BigqueryTable:
    def __init__(self, config_file):
        with open(config_file, 'r') as f:
            self.config_file = json.load(f)
            self.key_path = self.config_file["key_bg"]
            self.table_id = self.config_file["table_id"]
            self.directory = self.config_file['directory']
        self.client = self.get_client()

    def get_client(self) -> bigquery.Client:
        client = bigquery.Client.from_service_account_json(self.key_path)
        return client

    def create_bq_table(self, schema_file) -> None:
        with open(schema_file, 'r') as f:
            schema = json.load(f)
        table_config = bigquery.Table(self.table_id, schema=schema)
        self.client.create_table(table_config, exists_ok=True)

    def write_to_table(self):
        latest_file = max(glob.glob(os.path.join(self.directory, "*.json")),
                          key=os.path.getctime)  # latest file to find

        with open(latest_file, "r") as f:
            lines = f.readlines()
            rows = [json.loads(line) for line in lines]

        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        job = self.client.load_table_from_json(rows, self.table_id, job_config=job_config)
        job.result()
