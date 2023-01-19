from google.cloud import bigquery as bq
from Repository.GHBQAdaptor import GHBQAdaptor
import os


class BQ:
    def __init__(self, key):
        '''要放入要專案的Service Account'''
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key
        self.key = key
        self.client = bq.Client()
        self.raw_tables = {}
        self.table_names = []
        # 要複製的dataset
        self.source_project_id = ''  # 複製INFORMATION_SCHEMA 的DB
        self.source_dataset_id = ''
        # 新New dataset
        self.newdataset_id = ''
        self.destination_dataset_id = ''
        self.destination_project_id = ''  # 預設為VTC-SSR project

    def set_destination_id(self, pro_id, new_dataset):
        self.destination_project_id = pro_id
        self.destination_dataset_id = new_dataset

    def set_source_dataset(self, pro_id, dataset_source):
        self.source_project_id = pro_id
        self.source_dataset_id = dataset_source

    def create_dataset(self, new_dataset):
        dataset = bq.Dataset(new_dataset)
        dataset.location = "US"
        self.client.create_dataset(dataset, timeout=30)

    def create_table_schemas(self):
        tables = self.client.list_tables(self.source_dataset_id)
        for table in tables:
            self.raw_tables[table.table_id] = ''

        # find table schema
        for table_name in self.raw_tables:
            if 'view' in table_name or 'View' in table_name \
                    or 'user_info' in table_name \
                    or 'user_vip_level ' in table_name \
                    or 'vip_level' in table_name \
                    or 'withdraw_record' in table_name \
                    or 'offer_info' in table_name \
                    or 'member_info' in table_name \
                    or 'login_log' in table_name \
                    or 'deposit_record' in table_name \
                    or 'cash_entry_list' in table_name \
                    or 'bet_analysis' in table_name \
                    or 'cash_entry_list' in table_name \
                    or 'cash_entry_list' in table_name \
                    or 'deposit_record' in table_name \
                    or 'user_info' in table_name \
                    or 'game_code_dict' in table_name \
                    or 'lobby_dict' in table_name \
                    or 'login_log' in table_name \
                    or 'offer_info' in table_name \
                    or 'opcode_list' in table_name \
                    or 'withdraw_record' in table_name:
                    # in table_name:
                view_id = f"{self.source_dataset_id}.{table_name}"
                view = self.client.get_table(view_id)
                view_query = view.view_query.replace(self.source_dataset_id, self.destination_dataset_id)
                create_statement = f"create or replace View `{self.destination_dataset_id}.{table_name}` AS {view_query}"

            else:
                statement = f'SELECT ddl ' \
                            f'FROM `{self.source_dataset_id}`.INFORMATION_SCHEMA.TABLES ' \
                            f'WHERE table_name="{table_name}";'
                adaptor = GHBQAdaptor(self.key, self.source_dataset_id, table_name)
                create_statement = adaptor.show_table_schema(statement)
                create_statement = create_statement.replace(f"{self.source_dataset_id}", f"{self.destination_dataset_id}")
                create_statement = create_statement.replace(f"{self.source_project_id}", f'{self.destination_project_id}')

            self.raw_tables[table_name] = create_statement
