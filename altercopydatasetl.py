import json
from Repository import Get_SA, Dump_tables_schema
from Repository.SSRBQAdaptor import SSRBQAdaptor


def main(new_dataset, dataset_source, data_type):
    # 新增用的SA
    new_sa = Get_SA.SA(new_dataset)
    key = new_sa.service_account
    print(key)
    new_pro_id = new_sa.project_id
    # 來源用的SA
    source_sa = Get_SA.SA(dataset_source)
    source_key = source_sa.service_account
    source_pro_id = source_sa.project_id

    if data_type == 'create_tables':
        ssr_bq = Dump_tables_schema.BQ(key)  # 創建table帶入key
        ssr_bq.set_destination_id(new_pro_id, new_dataset)
        ssr_bq.set_source_dataset(source_pro_id, dataset_source)  # 取得要複製的dataset
        try:
            ssr_bq.create_dataset(new_dataset)
            print(f'{new_dataset} 新增成功')
        except:
            print(f'{new_dataset} 已經存在')

        ssr_bq.create_table_schemas() # 創建table

        with open("SSR_tables_create_statement.json", "w") as file:
            json.dump(ssr_bq.raw_tables, file)

        with open("SSR_tables_create_statement.json", "r") as file:
            create_table_dcts = json.load(file)

        for ssr_table, ssr_create_statement in create_table_dcts.items():
            try:
                ssr = SSRBQAdaptor(key, new_dataset, ssr_table)
                task = ssr.create_table(new_dataset, ssr_create_statement)
                print(f'{ssr_table} table 成功新增')

            except:
                print(f'{ssr_table} 出錯')
                pass


if __name__ == '__main__':
    # 複製來源'
    dataset_source = 'your_dataset.source_dataset'
    # 輸入要創建的Dataset 名稱
    new_dataset = 'destination_dataset.destination_dataset'
    # 複製方式
    data_type = 'create_tables'
    main(new_dataset, dataset_source, data_type)