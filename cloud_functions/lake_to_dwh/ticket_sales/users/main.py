import os

import functions_framework
from google.cloud import bigquery
from google.cloud import storage


@functions_framework.http
def load_data(request):
    # file_info
    bucket_name = f"{os.environ.get('ENV')}-lake-kohta"
    # 認証情報を指定してクライアントを作成する
    storage_client = storage.Client()

    # バケット名とファイル名を指定してファイルを取得する
    bucket = storage_client.get_bucket(bucket_name)

    # ジョブごとに固有のインプット情報
    folder_path = "ticket_sales/users/"
    file_prefix = "users"

    # ディレクトリ配下のオブジェクト一覧を取得する
    objects = bucket.list_blobs(prefix=folder_path)

    dataset_id = f"{os.environ.get('ENV')}_dwh"
    table_id = "users"
    client = bigquery.Client()
    dataset_ref = client.dataset(dataset_id)
    # Set Load Config
    job_config = bigquery.LoadJobConfig()
    job_config.autodetect = True
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.write_disposition = "WRITE_APPEND"

    def target_file_check():
        # 特定のフォルダのcsvファイル数のみをカウントするためにリスト化する
        csv_file_list = []
        for obj in objects:
            if obj.name.endswith(".csv") and "/done/" not in obj.name:
                csv_file_list.append(obj.name)
            else:
                continue

        # csv_file_listの要素数を確認して、1と2両方ともクリアしたら処理を進める
        # 1. 要素数が1つであること
        if len(csv_file_list) != 1:
            target_object_name = None
            target_file_name = None
            result = "NG"
            print("1度に複数のファイルが置かれています。1つずつ処理してください。")
            return target_object_name, target_file_name, result
        else:
            target_object_name = csv_file_list[0]
            target_file_name = target_object_name.split("/")[-1]
            target_file_prefix = target_file_name.removesuffix(
                "_" + target_object_name.split("/")[-1].rsplit("_")[-1]
            )
            # 2. ファイル名のプレフィックスが合致している場合は、file_nameとしてその文字列を保存
            if target_file_prefix == file_prefix:
                print("想定のプレフィックスと実際置かれたファイルのプレフィックスが一致したので、loadジョブを実行")
                result = "OK"
            else:
                print("ファイルのprefixが一致しません。配置したファイルを確認してください。")
                target_object_name = None
                result = "NG"
                return target_object_name, target_file_name, result
        return target_object_name, target_file_name, result

    # Load data
    target_object_name, target_file_name, result = target_file_check()
    if result == "OK":
        uri = f"gs://{bucket_name}/{target_object_name}"
        load_job = client.load_table_from_uri(
            uri, dataset_ref.table(table_id), job_config=job_config
        )
        print("Starting job {}".format(load_job.job_id))
        load_job.result()
        print("Job finished.")
    else:
        print(f"resultが{result}だったため、load処理は実行されませんでした")
        return result

    # Load完了したcsvをdoneフォルダ配下に移動
    src_path = folder_path + target_file_name
    dst_path = folder_path + "done/" + target_file_name
    try:
        # オブジェクトをコピーする
        blob = bucket.blob(src_path)
        new_blob = bucket.copy_blob(blob, bucket, dst_path)

        # 元のオブジェクトを削除する
        blob.delete()

        # コピーが成功したかどうかを確認する
        if new_blob.exists():
            print(f"Object {src_path} moved to {dst_path} successfully")
        else:
            print(f"Failed to move object {src_path} to {dst_path}")
        return result
    except Exception as e:
        print(f"Failed to move object {src_path} to {dst_path}: {e}")
