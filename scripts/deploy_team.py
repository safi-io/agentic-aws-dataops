"""Package, deploy, update, and manage the AWS-hosted team demo.

Uses the caller's AWS profile locally and an instance role on AWS. Source archives
use an explicit allowlist; local credentials, datasets, and other projects are
never included.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import shlex
import sys
import tarfile
import time

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


ROOT = Path(__file__).resolve().parents[1]
STATE = ROOT / ".deployment"
SOURCE_DIRECTORIES = ("config", "dagster_project", "dashboard", "dataops_agent", "deploy", "run_configs")
SOURCE_FILES = ("requirements.txt", "workspace.yaml")


def package_source() -> tuple[Path, str]:
    STATE.mkdir(exist_ok=True)
    archive = STATE / "source.tar.gz"
    paths = [ROOT / name for name in SOURCE_FILES]
    for name in SOURCE_DIRECTORIES:
        paths.extend(p for p in (ROOT / name).rglob("*") if p.is_file())
    source_suffixes = {".py", ".yml", ".yaml", ".json", ".txt", ".sh", ".service", ".html", ".css", ".js"}
    paths = sorted(
        p for p in paths
        if "__pycache__" not in p.parts
        and p.suffix in source_suffixes
        and not any(part.startswith(".") for part in p.relative_to(ROOT).parts)
    )
    with tarfile.open(archive, "w:gz") as target:
        for path in paths:
            if path.is_symlink() or not path.resolve().is_relative_to(ROOT):
                raise ValueError(f"Refusing source link outside the project: {path.name}")
            relative = path.relative_to(ROOT).as_posix()
            def normalized(info: tarfile.TarInfo) -> tarfile.TarInfo:
                info.mode = 0o644
                info.uid = info.gid = 0
                info.uname = info.gname = ""
                return info
            target.add(path, arcname=relative, recursive=False, filter=normalized)
    digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    print(f"Packaged {len(paths)} source files; {archive.stat().st_size:,} bytes.", flush=True)
    return archive, f"deployments/team/{digest}.tar.gz"


def outputs(cfn, stack: str) -> dict[str, str]:
    record = cfn.describe_stacks(StackName=stack)["Stacks"][0]
    return {item["OutputKey"]: item["OutputValue"] for item in record.get("Outputs", [])}


def wait_for_stack(cfn, stack: str) -> None:
    deadline = time.monotonic() + 1200
    previous = None
    while time.monotonic() < deadline:
        status = cfn.describe_stacks(StackName=stack)["Stacks"][0]["StackStatus"]
        if status != previous:
            print(f"Stack: {status}", flush=True)
            previous = status
        if status == "CREATE_COMPLETE":
            return
        if "FAILED" in status or "ROLLBACK" in status:
            raise RuntimeError(f"Stack deployment stopped with {status}; inspect CloudFormation events.")
        time.sleep(10)
    raise TimeoutError("Stack is still provisioning; use the status command to inspect it.")


def deploy(args, session, cfn) -> dict[str, str]:
    rds = session.client("rds")
    ec2 = session.client("ec2")
    ssm = session.client("ssm")
    database = rds.describe_db_instances(DBInstanceIdentifier=args.database)["DBInstances"][0]
    vpc = database["DBSubnetGroup"]["VpcId"]
    subnets = ec2.describe_subnets(Filters=[{"Name": "vpc-id", "Values": [vpc]}])["Subnets"]
    subnet = next(item["SubnetId"] for item in sorted(subnets, key=lambda s: s["AvailabilityZone"])
                  if item["MapPublicIpOnLaunch"] and item["AvailableIpAddressCount"] > 2)
    image = ssm.get_parameter(
        Name="/aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id",
    )["Parameter"]["Value"]
    template = (ROOT / "deploy/aws/template.yaml").read_text(encoding="utf-8")
    cfn.validate_template(TemplateBody=template)
    archive, key = package_source()
    session.client("s3").upload_file(str(archive), args.bucket, key, ExtraArgs={
        "ServerSideEncryption": "AES256", "ContentType": "application/gzip",
    })
    parameters = {
        "VpcId": vpc, "SubnetId": subnet, "ImageId": image,
        "InstanceType": args.instance_type, "DataBucket": args.bucket, "SourceKey": key,
        "DatabaseHost": database["Endpoint"]["Address"],
        "DatabaseSecretArn": database["MasterUserSecret"]["SecretArn"],
        "DatabaseSecurityGroup": database["VpcSecurityGroups"][0]["VpcSecurityGroupId"],
    }
    change_set = cfn.create_change_set(
        StackName=args.stack, ChangeSetName=f"initial-{int(time.time())}",
        ChangeSetType="CREATE", TemplateBody=template,
        Parameters=[{"ParameterKey": k, "ParameterValue": v} for k, v in parameters.items()],
        Capabilities=["CAPABILITY_IAM"],
        Tags=[{"Key": "Project", "Value": "AgenticDataOps"}],
    )["Id"]
    cfn.get_waiter("change_set_create_complete").wait(
        ChangeSetName=change_set, WaiterConfig={"Delay": 5, "MaxAttempts": 60},
    )
    planned = cfn.describe_change_set(ChangeSetName=change_set)
    for page in cfn.get_paginator("describe_events").paginate(
        ChangeSetName=change_set, Filters={"FailedEvents": True},
    ):
        if page.get("OperationEvents"):
            raise RuntimeError("CloudFormation reported failed pre-deployment events; inspect the change set.")
    for change in planned["Changes"]:
        item = change["ResourceChange"]
        if item["Action"] != "Add":
            raise RuntimeError("Initial deployment must only add resources.")
        print(f"Create {item['LogicalResourceId']} ({item['ResourceType']})", flush=True)
    cfn.execute_change_set(ChangeSetName=change_set)
    wait_for_stack(cfn, args.stack)
    result = outputs(cfn, args.stack)
    save_state(args, result)
    return result


def save_state(args, result: dict[str, str]) -> None:
    STATE.mkdir(exist_ok=True)
    (STATE / "team-state.json").write_text(json.dumps({
        "stack": args.stack, "region": args.region, "bucket": args.bucket, **result,
    }, indent=2), encoding="utf-8")


def update(args, session, result) -> None:
    archive, key = package_source()
    session.client("s3").upload_file(str(archive), args.bucket, key, ExtraArgs={
        "ServerSideEncryption": "AES256", "ContentType": "application/gzip",
    })
    command = shlex.join([
        "/opt/dataops/venv/bin/python", "/opt/dataops/app/deploy/aws/update_server.py",
        "--bucket", args.bucket, "--key", key,
    ])
    ssm = session.client("ssm")
    command_id = ssm.send_command(
        InstanceIds=[result["InstanceId"]], DocumentName="AWS-RunShellScript",
        Parameters={"commands": [command], "executionTimeout": ["1200"]},
        Comment="Update DataOps from reviewed repository source",
    )["Command"]["CommandId"]
    print(f"Update command: {command_id}", flush=True)
    deadline = time.monotonic() + 1250
    while time.monotonic() < deadline:
        try:
            response = ssm.get_command_invocation(CommandId=command_id, InstanceId=result["InstanceId"])
        except ssm.exceptions.InvocationDoesNotExist:
            time.sleep(5)
            continue
        if response["Status"] == "Success":
            print("Source update completed.")
            return
        if response["Status"] in {"Failed", "TimedOut", "Cancelled", "Cancelling"}:
            print(response["StandardErrorContent"])
            raise RuntimeError(f"Source update {response['Status']}; inspect SSM command {command_id}.")
        time.sleep(10)
    raise TimeoutError(f"Inspect SSM command {command_id} for the update result.")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["package", "deploy", "update", "status", "login", "stop", "start"])
    parser.add_argument("--profile", default="default")
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--stack", default="agentic-dataops-team")
    parser.add_argument("--bucket", default="agentic-dataops-731451715213-us-east-1")
    parser.add_argument("--database", default="agentic-dataops-postgres")
    parser.add_argument("--instance-type", default="t3a.large")
    args = parser.parse_args()
    if args.action == "package":
        package_source()
        return 0
    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    cfn = session.client("cloudformation", config=Config(
        retries={"mode": "standard", "total_max_attempts": 4},
        connect_timeout=10, read_timeout=30,
    ))
    try:
        result = deploy(args, session, cfn) if args.action == "deploy" else outputs(cfn, args.stack)
        save_state(args, result)
        if args.action == "update":
            update(args, session, result)
        elif args.action == "login":
            login = json.loads(session.client("secretsmanager").get_secret_value(
                SecretId=result["LoginSecretArn"],
            )["SecretString"])
            target = STATE / "team-access.txt"
            target.write_text(
                f"Team link: {result['TeamUrl']}\nUsername: {login['username']}\n"
                f"Password: {login['password']}\n\nShare privately with your teammates. "
                "This file is excluded from Git and source deployments.\n",
                encoding="utf-8",
            )
            target.chmod(0o600)
            print(f"Login saved privately to {target}")
        elif args.action in {"stop", "start"}:
            client = session.client("ec2")
            operation = client.stop_instances if args.action == "stop" else client.start_instances
            operation(InstanceIds=[result["InstanceId"]])
            print(f"Requested server {args.action}. Disk and reserved IP charges continue while stopped.")
        print(json.dumps(result, indent=2))
        if args.action == "deploy":
            print("Cloud infrastructure is ready. Allow bootstrap to finish before sharing the URL.")
        return 0
    except ClientError as exc:
        print(f"AWS error: {exc.response['Error']['Code']}: {exc.response['Error']['Message']}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
