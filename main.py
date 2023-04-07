from handler import CSVAnalyzer
from pathlib import Path
from bq_utils import BigqueryTable

if __name__ == '__main__':
    config_file = Path('/home/kote/PycharmProjects/scv_stats_generator/config.json')
    schema_file = Path('/home/kote/PycharmProjects/scv_stats_generator/schema.json')
    table = BigqueryTable(config_file)
    table.create_bq_table(config_file, schema_file)
    csvgen = CSVAnalyzer(config_file)
    csvgen.analyze_files()
    csvgen.write_to_json()
