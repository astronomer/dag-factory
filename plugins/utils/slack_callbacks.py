import functools
import logging
import re

from plugins.utils import get_namespace, get_environment

logger = logging.getLogger(__file__)


def _get_url(log_url, namespace, environment):
    """
    Temporary helper function to format the log url.
    Ideally the correct host url would be set in the helm chart.
    """
    host = f"https://airflow.{namespace}.{environment}.k8s.pelotime.com"
    return log_url.replace("http://localhost:8080", host)


# allowing slack alerts on Joey and Guy's namespace for testing.
NAMESPACE_TO_SLACK = {
    "airflow": "slack",
    "guyfeldman": "slack_guyfeldman",
    "zhaoyuluo": "slack_luozhaoyu",
    "terryyin": "slack_terryyin",
    "annajuchnicki": "slack_annajuchnicki"
}

START_COLOR = "#01FF70"
SUCCESS_COLOR = "0F7000"
FAILURE_COLOR = "#ff0000"
RETRY_COLOR = "#fed136"

ALERT_TYPE_TO_COLOR = {
    "start": START_COLOR,
    "success": SUCCESS_COLOR,
    "failure": FAILURE_COLOR,
    "retry": RETRY_COLOR,
    "alert": RETRY_COLOR
}

ALERT_TYPE_TO_STATUS = {
    "start": "Started",
    "success": "Succeeded",
    "failure": "Failed",
    "retry": "Failed",
    "alert": "Alert"
}


def _make_header(text, text_type="plain_text", text_emoji=True):
    return {
        "type": "header",
        "text": {
            "type": text_type,
            "text": text,
            "emoji": text_emoji
        }
    }


def _make_section(text, text_type="mrkdwn"):
    return {
        "type": "section",
        "text": {
            "type": text_type,
            "text": text
        }
    }


def _add_section_accessory(
        base_section,
        accessory_url,
        accessory_type="button",
        accessory_text_type="plain_text",
        accessory_text_text=":airflow: Logs",
        accessory_text_emoji=True,
        accessory_value="airflow_logs",
        accessory_action_id="button-action"

):
    base_section['accessory'] = {
        "type": accessory_type,
        "text": {
            "type": accessory_text_type,
            "text": accessory_text_text,
            "emoji": accessory_text_emoji,
        },
        "value": accessory_value,
        "url": accessory_url,
        "action_id": accessory_action_id,
    }
    return base_section


def _make_context(texts: list, context_element_types: list):
    return {
        "type": "context",
        "elements": [{"type": tp, "text": tx} for tp, tx in zip(context_element_types, texts)]
    }


def _slack_alert(
        context,
        mentions=None,
        namespace=None,
        slack_conn_id=None,
        alert_type="failure",
        level="task",
        maintenance_window_violated=False
):
    slack_conn_id = slack_conn_id or NAMESPACE_TO_SLACK.get(namespace)
    if slack_conn_id is None:
        return
    environment = get_environment()
    ti = context.get("task_instance")
    try:
        error = context.get('exception')
        if ti.operator == 'KubernetesPodOperator':
            error = re.search(r'Pod .+ returned a failure', str(error)).group(0)
    except Exception as e:
        error = None

    try_number = ti.try_number - 1
    max_attempts = ti.max_tries + 1
    color = ALERT_TYPE_TO_COLOR[alert_type]

    task_status_text = (
        level.lower().capitalize() + " " + ALERT_TYPE_TO_STATUS[alert_type]
    )
    if level == "task":
        header = _make_header(
            text="{task}: {task_status_text}".format(task=ti.task_id, task_status_text=task_status_text)
        )
        main_section_base = _make_section(
            text="*Dag*: `{dag}`\n*Attempt*: `{try_number} out of {max_attempts}` \n*Execution Time*: `{exec_date}`".format(
                dag=ti.dag_id,
                namespace=namespace,
                exec_date=context.get("execution_date"),
                try_number=try_number,
                max_attempts=max_attempts,
            )
        )
        main_section = _add_section_accessory(
            main_section_base,
            accessory_url=_get_url(
                ti.log_url,
                namespace=namespace,
                environment=environment,
            )
        )
    else:
        header = _make_header(
            text=task_status_text
        )
        main_section_base = _make_section(text="*Dag*: `{dag}`".format(dag=ti.dag_id))
        main_section = _add_section_accessory(
            main_section_base,
            accessory_url=f"https://airflow.{namespace}.{environment}.k8s.pelotime.com/tree?dag_id={ti.dag_id}"
        )

    blocks = [
        header,
        main_section
    ]

    if error:
        blocks.append(
            _make_section(text="*Failure Reason*: ```{error}```".format(error=str(error)))
        )

    if alert_type == 'success':
        success_message = ti.xcom_pull(key='success_message')
        if success_message:
            blocks.append(_make_section(text=f"*Success Message:* ```{success_message}```"))

    if maintenance_window_violated:
        msg = "Invalid 'schedule_interval',please avoid 0 am to 2 am as the period is reserved for Redshift maintenance." \
              "If you are certain that the dag won't read or write to Redshift and you do need to schedule in themaintenance window, " \
              "add an exemption tag to the dag. eg. tags=['exempt_block_window']"
        blocks.append(_make_section(text=msg))

    if mentions:
        blocks.append(
            _make_context([" ".join(mentions) if mentions else " "], ["mrkdwn"])
        )
    slack_payload = [{"color": color, "blocks": blocks}]
    send_slack(context=context, attachments=slack_payload, http_conn_id=slack_conn_id)


def send_slack(http_conn_id=None, context=None, *args, **kwargs):
    if not http_conn_id:
        http_conn_id = NAMESPACE_TO_SLACK[get_namespace()]
    try:
        from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
        hook = SlackWebhookHook(
            http_conn_id=http_conn_id,
            username="airflow",
            *args,
            **kwargs,
        )
        hook.execute()
    except Exception as e:
        logger.exception(
            "Could not send slack alert for task {} due to:{}".format(context, e)
        )


def task_fail_slack_alert(slack_conn_id=None, mentions=None):
    namespace = get_namespace()
    if isinstance(mentions, str):
        mentions = [mentions]
    return functools.partial(
        _slack_alert,
        alert_type="failure",
        level="task",
        slack_conn_id=slack_conn_id,
        mentions=mentions,
        namespace=namespace
    )


def task_retry_slack_alert(slack_conn_id=None, mentions=None):
    namespace = get_namespace()
    if isinstance(mentions, str):
        mentions = [mentions]
    return functools.partial(
        _slack_alert,
        alert_type="retry",
        level="task",
        slack_conn_id=slack_conn_id,
        mentions=mentions,
        namespace=namespace
    )


def task_success_slack_alert(slack_conn_id=None, mentions=None):
    namespace = get_namespace()
    if isinstance(mentions, str):
        mentions = [mentions]
    return functools.partial(
        _slack_alert,
        alert_type="success",
        level="task",
        slack_conn_id=slack_conn_id,
        mentions=mentions,
        namespace=namespace,
    )


def dag_start_slack_alert(slack_conn_id=None, mentions=None):
    namespace = get_namespace()
    if isinstance(mentions, str):
        mentions = [mentions]
    return functools.partial(
        _slack_alert,
        alert_type="start",
        level="dag",
        slack_conn_id=slack_conn_id,
        mentions=mentions,
        namespace=namespace,
    )


def dag_success_slack_alert(slack_conn_id=None, mentions=None):
    namespace = get_namespace()
    if isinstance(mentions, str):
        mentions = [mentions]
    return functools.partial(
        _slack_alert,
        alert_type="success",
        level="dag",
        slack_conn_id=slack_conn_id,
        mentions=mentions,
        namespace=namespace,
    )


def maintenance_window_violate_slack_alert(slack_conn_id=None, mentions=None):
    namespace = get_namespace()
    if isinstance(mentions, str):
        mentions = [mentions]
    return functools.partial(
        _slack_alert,
        alert_type="alert",
        level="dag",
        slack_conn_id=slack_conn_id,
        mentions=mentions,
        namespace=namespace,
        maintenance_window_violated=True
    )