from bigquery.BigQuery import BigQueryAdaptor, BigQueryConnection, BigQueryParams
import Environment as envi


class GHBQAdaptor(BigQueryAdaptor):
    def __init__(self, key, dataset_name, table_name):
        super().__init__(BigQueryConnection(BigQueryParams(cert_path=key)))
        # super().__init__(BigQueryConnection(BigQueryParams(cert_path=envi.SSR_BQ_KEY)))
        self.dataset_name = dataset_name
        self.table_name = table_name

    def show_table_schema(self, create_statement):
        self.mode = self.QUERY_MODE
        self.statement = create_statement
        self.exec()
        df = self.fetch_data
        create_schema = df.iloc[0].ddl
        return create_schema


