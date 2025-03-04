from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

def create_slack_webhook(task_id, message):
    return SlackWebhookOperator(
        task_id=task_id,
        slack_webhook_conn_id="slack_default",
        message=(message)
    )

def task_success_alert(context):
    task_id = "task_success"
    message = f"""
:large_blue_circle: <{context.get('task_instance').log_url}|Task Successed> {context.get('task_instance')} 
>   `DAG`: {context.get('task_instance').dag_id} 
>   `Run Id`: {context.get('run_id')} 
>   `Task``: {context.get('task_instance').task_id}  
>   `Execution`: {context.get('execution_date')} 
"""
    
    slack_webhook = create_slack_webhook(task_id, message)
    return slack_webhook.execute(context=context)

def task_failure_alert(context):
    task_id = "task_failure"
    message = f"""
:red_circle: <{context.get('task_instance').log_url}|Task Failed> {context.get('task_instance')} 
>   `DAG`: {context.get('task_instance').dag_id} 
>   `Run Id`: {context.get('run_id')} 
>   `Task``: {context.get('task_instance').task_id}  
>   `Execution`: {context.get('execution_date')} 
>
> ```{context.get('exception')}``` 
"""
    
    slack_webhook = create_slack_webhook(task_id, message)
    return slack_webhook.execute(context=context)

def dag_success_alert(context):
    task_id = "dag_success"
    message = f"""
:large_blue_circle: <{context.get('task_instance').log_url}|DAG Successed> {context.get('dag')} 
>   `DAG`: {context.get('dag').dag_id} 
>   `Run Id`: {context.get('run_id')} 
>   `Execution`: {context.get('execution_date')} 
"""

    slack_webhook = create_slack_webhook(task_id, message)
    return slack_webhook.execute(context=context)

def dag_failure_alert(context):
    task_id = "dag_failure"
    message = f"""
:red_circle: <{context.get('task_instance').log_url}|DAG Failed> {context.get('dag')} 
>   `DAG`: {context.get('dag').dag_id} 
>   `Run Id`: {context.get('run_id')} 
>   `Execution`: {context.get('execution_date')} 
>
> ```{context.get('exception')}``` 
"""
    
    slack_webhook = create_slack_webhook(task_id, message)
    return slack_webhook.execute(context=context)
