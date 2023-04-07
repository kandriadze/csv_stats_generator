import os
import pandas as pd
import json
from datetime import datetime


class CSVAnalyzer:
    def __init__(self, config_file):
        with open(config_file, 'r') as f:
            self.config_file = json.load(f)
            self.directory = self.config_file['directory']
            self.summary = {}

    def analyze_files(self):
        for file in os.listdir(self.directory):
            if file.endswith('.csv'):
                filepath = os.path.join(self.directory, file)
                df = pd.read_csv(filepath)
                file_size_mb = os.path.getsize(filepath) / (1024 * 1024)
                num_rows = len(df)
                num_cols = len(df.columns)
                size_one_col_mb = df.memory_usage(deep=True).sum() / (1024 * 1024 * num_cols)
                size_df_mb = df.memory_usage(deep=True).sum() / (1024 * 1024)
                stat = os.stat(filepath)
                created_time = datetime.fromtimestamp(stat.st_ctime).isoformat(' ', 'seconds')
                modified_time = datetime.fromtimestamp(stat.st_mtime).isoformat(' ', 'seconds')
                self.summary[file] = {
                    'file_name': file.split(".")[0],
                    'file_size_mb': round(file_size_mb, 2),
                    'num_rows': num_rows,
                    'num_cols': num_cols,
                    'size_one_col_mb': round(size_one_col_mb, 2),
                    'size_df_mb': round(size_df_mb, 2),
                    'created': created_time,
                    "modified": modified_time
                }

    def write_to_json(self):
        now = datetime.now()
        timestamp = now.isoformat(' ', 'seconds')
        output_file = f"summary_{timestamp}.json"
        df = pd.DataFrame(self.summary).T
        df.index.name = 'file'
        with open(output_file, 'r') as f:
            old_df = pd.read_json(f, orient='records', lines=True)
            old_df.set_index('file', inplace=True)

            for file in df.index:
                if file in old_df.index:
                    if df.loc[file].equals(old_df.loc[file]):
                        df.at[file, 'modified'] = old_df.loc[file, 'modified']
                    else:
                        df.at[file, 'modified'] = timestamp
                else:
                    df.at[file, 'modified'] = timestamp

        for file in df.index:
            df.at[file, 'modified'] = timestamp

        df.to_json(output_file, orient='records', lines=True, double_precision=2)
