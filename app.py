#!/usr/bin/env python3
import os

import aws_cdk as cdk
from cicd import cicdflow
from dagster_s3_sample.s3_sample import SampleStack

tags = {
    "Name": "dagster-s3-sample",
    "NameRepository": os.getenv("CI_PROJECT_NAME"),
    "RepositoryLocation": "GitLab",
    "NameTeam": "MDP",
    "Expiry": "never",  # Can be never for master, dmz or release branches
    "DeploymentBranch": os.getenv("CI_COMMIT_BRANCH"),
    "StackOwnerPersonalID": "063209"  # HDI Personal ID
}

stacks = [
    {
        "name": "dagster-s3-sample",
        "description": "dagster-s3-sample",
        "module": SampleStack,
        "tags": tags,
        "depends_on": {}
    },
]

app = cdk.App()
cicdflow.build_stacks(app, stacks)
app.synth()
