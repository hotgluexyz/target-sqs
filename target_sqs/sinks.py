"""SQS target sink class, which handles writing streams."""


import logging
import uuid

import boto3
import json
from botocore.exceptions import ClientError
from singer_sdk.sinks import BatchSink

logger = logging.getLogger(__name__)


class SQSSink(BatchSink):
    """SQS target sink class."""

    sqs = None
    max_size = 10000  # Max records to write in one batch

    def get_sqs(self):
        if self.sqs is None:
            self.sqs = boto3.resource(
                "sqs",
                aws_access_key_id=self.config.get("aws_access_key"),
                aws_secret_access_key=self.config.get("aws_secret_key"),
                region_name=self.config.get("aws_region", "us-east-1"),
            )
        return self.sqs

    def get_queue(self, name):
        sqs = self.get_sqs()
        try:
            queue = sqs.get_queue_by_name(QueueName=name)
            logger.info("Got queue '%s' with URL=%s", name, queue.url)
        except ClientError as error:
            logger.exception("Couldn't get queue named %s.", name)
            raise error
        else:
            return queue

    def send_messages(self, messages):
        queue = self.get_queue(self.config.get("queue_name"))
        try:
            entries = [
                {
                    "Id": str(uuid.uuid4()),
                    "MessageBody": json.dumps(msg, default=str)
                }
                for msg in messages
            ]
            response = queue.send_messages(Entries=entries)
            if "Successful" in response:
                for msg_meta in response["Successful"]:
                    logger.info(
                        "Message sent: %s: %s",
                        msg_meta["MessageId"],
                        messages[int(msg_meta["Id"])]["body"],
                    )
            if "Failed" in response:
                for msg_meta in response["Failed"]:
                    logger.warning(
                        "Failed to send: %s: %s",
                        msg_meta["MessageId"],
                        messages[int(msg_meta["Id"])]["body"],
                    )
        except ClientError as error:
            logger.exception("Send messages failed to queue: %s", queue)
            raise error
        else:
            return response

    def process_batch(self, context: dict) -> None:
        self.send_messages(context.get("records"))
