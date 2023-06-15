import os
import json

from google.cloud import firestore
from google.cloud import pubsub_v1


def get_params(event, context):
    # check content-type
    if event["contentType"] != "text/csv":
        print(f"Not supported file type: {event['contentType']}")
        return

    db = firestore.Client(project="test-project-kohta")

    users_ref = db.collection(f"{os.environ.get('ENV')}-param")
    docs = users_ref.stream()

    input_bucket_name = event["bucket"]
    input_object_name = event["name"]
    input_folder_path = input_object_name.rsplit("/", 1)[0]
    input_file_prefix = input_object_name.split("/")[-1].rsplit("_", 1)[0]

    # Publisherの作成
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(
        "test-project-kohta", f"{os.environ.get('ENV')}_load_topic"
    )

    for doc in docs:
        bucket_name = doc.to_dict().get("bucket")
        folder_path = doc.to_dict().get("folder_path")
        file_prefix = doc.to_dict().get("file_prefix")
        if (
            bucket_name == input_bucket_name
            and folder_path == input_folder_path
            and file_prefix == input_file_prefix
        ):
            dict_data = {
                "following_workflow_name": doc.to_dict().get("following_workflow_name"),
            }
            data = json.dumps(dict_data).encode("utf-8")
            future = publisher.publish(topic_path, data=data)
            print(f"後続ワークフロー：{doc.to_dict().get('following_workflow_name')}")
            break
        else:
            continue
