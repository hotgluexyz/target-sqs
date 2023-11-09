"""SQS target sink class, which handles writing streams."""

import os
import logging
import uuid

import boto3
import json
from botocore.exceptions import ClientError
from singer_sdk.sinks import BatchSink
import sys

logger = logging.getLogger(__name__)
MAX_SIZE_IN_BYTES = 262_144

class SQSSink(BatchSink):
    """SQS target sink class."""

    sqs = None
    max_size = 10  # Max records to write in one batch

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
    def check_response(self,response):
        logger.info(
                "SQS Response: %s",
                response
            )
        if "Successful" in response:
            logger.info(
                "Successfully sent %s messages",
                len(response["Successful"])
            )
        if "Failed" in response:
            logger.warning(
                "Failed to send %s messages",
                len(response["Failed"])
            )
        #For now logging error. Will fail job if required.    
        if "Error" in response:
            if "Message" in response['Error']:
                logger.warning(
                    "Message not with message:  %s",
                    len(response['Error']["Message"])
                )
            if "Code" in response['Error']:
                logger.warning(
                    "ErrorCode:  %s",
                    len(response['Error']["Code"])
                )
            if "Code" and "Message" in response:
                raise Exception(f"Error sending message: {response['Error']['Code']} - {response['Error']['Message']}")


    def get_size(self,obj):
        total_size = 0 
        if type(obj) == list:
            for o in obj: 
                for key in o.keys():
                    total_size += sys.getsizeof(o[key])
            return total_size 
        elif type(obj) == dict:
            for key in obj.keys():
                total_size += sys.getsizeof(obj[key])
            
            return total_size

    def send_messages(self, messages):
        queue = self.get_queue(self.config.get("queue_name"))
        try:
            # Create the entries
            entries = []
            for msg in messages:
                entry_id = str(uuid.uuid4())
                # Inject the stream name
                msg['stream'] = self.stream_name

                if self.config.get("include_job_id", False):
                    msg['job_id'] = os.environ.get('JOB_ID')

                entry = {
                    "Id": entry_id,
                    "MessageBody": json.dumps(msg, default=str)
                }

                if self.config.get("path_prefix") is not None:
                    entry["MessageGroupId"] = self.config.get("path_prefix")
                    entry["MessageDeduplicationId"] = entry_id
                entries.append(entry)

            # Send the messages
            total_size = self.get_size(entries)
            
            # if size of all entries 
            if total_size > MAX_SIZE_IN_BYTES:
                batch = []
                for entry in entries: 
                    if self.get_size(batch) + self.get_size(entry) > MAX_SIZE_IN_BYTES:
                        # if current entry will tip over edge, flush current batch and add current entry in for next iterations
                        response = queue.send_messages(Entries=batch)
                        self.check_response(response)
                        logger.info(f"Sending batch of size {self.get_size(batch)} : {len(batch)} records")
                        batch = [entry]
                    else:
                        batch.append(entry)
                logger.info(f"Sending batch of size {self.get_size(batch)} : {len(batch)} records")
                response = queue.send_messages(Entries=batch)
            else: 
                logger.info(f"Sending all entries in one message with size {total_size} : {len(entries)} records")
                response = queue.send_messages(Entries=entries)
                self.check_response(response)

            # Check response
        except ClientError as error:
            logger.exception("Send messages failed to queue: %s", queue)
            raise error
        else:
            return response

    def process_batch(self, context: dict) -> None:
       self.send_messages(context.get("records"))
