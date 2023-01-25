from aws_cdk import (
    Stack,
    aws_ec2 as ec2
)
from constructs import Construct


class SampleStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, env_config, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        app_config_settings = env_config["app_config"]["Settings"]
        vpc_id = app_config_settings.get("vpc_id")
        subnet_id_list = app_config_settings.get("subnet_id").split(",")
        availability_zone_list = app_config_settings.get("availability_zone").split(",")

        imported_vpc = ec2.Vpc.from_vpc_attributes(
            self,
            id="ImportedVPC",
            availability_zones=availability_zone_list,
            vpc_id=vpc_id,
            private_subnet_ids=subnet_id_list
        )

        # The code that defines your stack goes here
