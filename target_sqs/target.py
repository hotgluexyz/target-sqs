"""SQS target class."""

from singer_sdk import typing as th
from singer_sdk.target_base import Target

from target_sqs.sinks import SQSSink


class TargetSQS(Target):
    """Sample target for SQS."""

    name = "target-sqs"
    config_jsonschema = th.PropertiesList(
        th.Property("aws_access_key", th.StringType, required=True),
        th.Property("aws_secret_key", th.StringType, required=True),
        th.Property("queue_name", th.StringType, required=True),
        th.Property(
            "aws_region",
            th.StringType,
        ),
    ).to_dict()
    default_sink_class = SQSSink


if __name__ == "__main__":
    TargetSQS.cli()
