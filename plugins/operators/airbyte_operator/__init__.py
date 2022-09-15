import boto3
import requests

from typing import TYPE_CHECKING, Sequence

from airflow.models import BaseOperator
from airflow.providers.airbyte.hooks.airbyte import AirbyteHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class AirbyteTriggerSyncOperator(BaseOperator):
    """
    This operator allows you to submit a job to an Airbyte server to run a integration
    process between your source and destination.
    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AirbyteTriggerSyncOperator`
    :param airbyte_conn_id: Required. The name of the Airflow connection to get connection
        information for Airbyte.
    :param connection_id: Optional. The Airbyte ConnectionId UUID between a source and destination.
        Mush provide connection_name if left empty.
    :param connection_name: Optional. The Airbyte Connection name which will be use for fetch ConnectionId.
    :param asynchronous: Optional. Flag to get job_id after submitting the job to the Airbyte API.
        This is useful for submitting long running jobs and
        waiting on them asynchronously using the AirbyteJobSensor.
    :param api_version: Optional. Airbyte API version.
    :param wait_seconds: Optional. Number of seconds between checks. Only used when ``asynchronous`` is False.
    :param timeout: Optional. The amount of time, in seconds, to wait for the request to complete.
        Only used when ``asynchronous`` is False.
    """

    template_fields: Sequence[str] = ('connection_id', 'connection_name')

    def __init__(
            self,
            connection_id: str | None,
            connection_name: str | None,
            airbyte_conn_id: str = "airbyte_default",
            asynchronous: bool | None = False,
            api_version: str = "v1",
            wait_seconds: float = 3,
            timeout: float | None = 3600,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.airbyte_conn_id = airbyte_conn_id
        self.connection_id = self.get_conn_id(connection_name) if connection_name else connection_id
        self.timeout = timeout
        self.api_version = api_version
        self.wait_seconds = wait_seconds
        self.asynchronous = asynchronous

    def execute(self, context: Context) -> None:
        """Create Airbyte Job and wait to finish"""
        self.hook = AirbyteHook(airbyte_conn_id=self.airbyte_conn_id, api_version=self.api_version)
        job_object = self.hook.submit_sync_connection(connection_id=self.connection_id)
        self.job_id = job_object.json()['job']['id']

        self.log.info("Job %s was submitted to Airbyte Server", self.job_id)
        if not self.asynchronous:
            self.log.info('Waiting for job %s to complete', self.job_id)
            self.hook.wait_for_job(job_id=self.job_id, wait_seconds=self.wait_seconds, timeout=self.timeout)
            self.log.info('Job %s completed successfully', self.job_id)

        return self.job_id

    def on_kill(self):
        """Cancel the job if task is cancelled"""
        if self.job_id:
            self.log.info('on_kill: cancel the airbyte Job %s', self.job_id)
            self.hook.cancel_job(self.job_id)

    def get_conn_id(self, connection_name):
        airbyte_api = "https://airbyte.airbyte.prod.k8s.pelotime.com/api"
        secret_name = "prod/data-engineering/airbyte/cli-credentials"
        region_name = "us-east-1"

        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=region_name
        )

        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )

        secret = get_secret_value_response['SecretString']
        token = secret.split(':')[2].strip()

        url = airbyte_api + "/v1/connections/search"

        headers = {"Authorization": token}

        body = {
            "namespaceDefinition": "customformat",
            "name": connection_name
        }

        resp = requests.post(url=url, json=body, headers=headers)
        data = resp.json()
        return data['connections'][0]["connectionId"]