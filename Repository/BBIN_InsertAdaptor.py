from bigquery.BigQuery import BigQueryAdaptor, BigQueryConnection, BigQueryParams


class BBINInsertAdaptor(BigQueryAdaptor):
    def __init__(self, key, new_dataset, dataset_source, domain_id):
        super().__init__(BigQueryConnection(BigQueryParams(cert_path=key)))
        self.table_name = ''
        self.new_dataset = new_dataset
        self.dataset_source = dataset_source
        self.hall_id = domain_id

    def put_hall_id(self, hall_id):
        self.hall_id = hall_id

    def insert_user_tag_data(self):
        sql_command = f"""insert into `{self.new_dataset}.user_tag_data`
                      SELECT
                      * 
                      from `{self.dataset_source}.user_tag_data` user_tag_data
                   """

        self.mode = self.QUERY_MODE
        self.statement = sql_command
        self.exec()
        print('insert_user_tag_data 成功')

    def insert_user_tag_info(self):
        sql_command = f"""insert into `{self.new_dataset}.user_tag_info`
                      SELECT
                      * 
                      from `{self.dataset_source}.user_tag_info` user_tag_info
                   """
        self.mode = self.QUERY_MODE
        self.statement = sql_command
        self.exec()
        print('insert_user_tag_info 成功')

    def insert_user_tag_ods_data(self):
        sql_command = f"""insert into `{self.new_dataset}.user_tag_ods_data`
                      SELECT
                      * 
                      from `{self.dataset_source}.user_tag_ods_data` user_tag_ods_data
                   """
        print(sql_command)
        self.mode = self.QUERY_MODE
        self.statement = sql_command
        self.exec()
        print('insert_user_tag_ods_data 成功')