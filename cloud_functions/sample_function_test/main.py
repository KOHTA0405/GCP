# ローカル環境での関数のテストサンプル
import functions_framework


# イベントドリブン関数の場合
@functions_framework.cloud_event
def hello(event):
    # print("Received", context.event_id)
    return event["data"]


# httpトリガーの場合
# @functions_framework.http
# def hello(request):
#     return "Hello World!"
