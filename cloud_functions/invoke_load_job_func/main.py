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

    # pubsubから、実行する後続のworkflow_nameを取得
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
