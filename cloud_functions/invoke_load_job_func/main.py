import base64
import json
from google.cloud import functions_v1
from google.cloud import workflows_v1
from google.cloud.workflows import executions_v1
from google.cloud.workflows.executions_v1 import Execution
from google.cloud.workflows.executions_v1.types import executions

functions_client = functions_v1.CloudFunctionsServiceClient()


def invoke_load_job(event, context):
    pubsub_message = base64.b64decode(event["data"]).decode("utf-8")
    print(f"event: {event}")
    print(f"eventのデータ型: {type(event)}")
    print(f"event['data']: {event['data']}")
    print(f"event['data']のデータ型: {type(event['data'])}")
    # print(f"following_function_name: {event['data'].get('following_function_name')}") <- エラー
    print(f"pubub_message: {pubsub_message}")
    print(f"pubub_messageのデータ型: {type(pubsub_message)}")
    print(f"pubub_messageのdict変換: {json.loads(pubsub_message)}")
    print(
        f"dictにして、following_workflow_nameを取得: {json.loads(pubsub_message).get('following_workflow_name')[0]}"
    )
    workflow_name = json.loads(pubsub_message).get("following_workflow_name")[0]

    # Set up API clients.
    execution_client = executions_v1.ExecutionsClient()
    workflows_client = workflows_v1.WorkflowsClient()

    # Construct the fully qualified location path.
    parent = workflows_client.workflow_path(
        "test-project-kohta", "us-east1", workflow_name
    )

    # Execute the workflow.
    response = execution_client.create_execution(request={"parent": parent})
    print(f"Created execution: {response.name}")
