import requests
from datetime import datetime

SLACK_WEBHOOK = ""
HEADERS = {
    "Content-type": "application/json",
}

def _send_msg(text):
    json_data = {
        "text": text,
    }

    response = requests.post(
        SLACK_WEBHOOK,
        headers=HEADERS,
        json=json_data,
    )

def dag_success_alert(context):
    text = f"DAG has succeeded, run_id: {context['run_id']}"

    _send_msg(text)

def task_failure_alert(context):
    text = f"Task has failed, task_instance_key_str: {context['task_instance_key_str']}"

    _send_msg(text)

if __name__ == "__main__":
    _send_msg("Hello, World!")