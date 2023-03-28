import os
import pandas as pd
import json


class CSVGenerator:
    def __init__(self, config_file):
        with open(config_file, 'r') as f:
            self.config_file = json.load(f)
            self.directory = self.config['directory']
            self.summray = {}

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
                self.summray[file] = {
                    'file_size_mb': round(file_size_mb, 2),
                    'num_rows': num_rows,
                    'num_cols': num_cols,
                    'size_one_col_mb': round(size_one_col_mb, 2),
                    'size_df_mb': round(size_df_mb, 2)
                }

    def write_to_json(self):
        with open(os.path.join(self.directory, 'summary_{timestamp}.json'), 'w') as f:
            json.dump(self.summray, f, indent=4)
