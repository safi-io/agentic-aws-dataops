# Shared AWS team demo

Dagster, its daemon, Streamlit, and Strands Decider run on one AWS EC2 server.
Your PC can be turned off. CSVs and clean/quarantined outputs use the existing
S3 bucket, incidents use the existing RDS database, and Dagster history and model
files live on the server's encrypted disk.

## Access and experiments

Current deployment:

- [Team home](https://44.196.49.100.sslip.io/)
- [Dagster](https://44.196.49.100.sslip.io/dagster/)
- [Incident dashboard](https://44.196.49.100.sslip.io/dashboard/)

```powershell
python scripts/deploy_team.py status
python scripts/deploy_team.py login
```

The first command prints the HTTPS links. The second saves the shared login to
`.deployment/team-access.txt`, excluded from Git and deployment packages. Share
the link and login privately with teammates. They need only a browser.

The landing page links to both apps and includes a copyable run configuration.
In Dagster, open **Jobs → orders_data_quality_job → Launchpad**, then launch:

```yaml
ops:
  bronze_orders:
    config:
      source_key: bronze/orders.csv
  silver_orders:
    config:
      source_key: bronze/orders.csv
```

For broken data, replace both keys with `bronze/high_nulls.csv` or
`bronze/unexpected_column.csv`. Failed validation is expected; refresh the
incident dashboard to see the recorded failure. Existing 1,100-row files:

- Clean: `bronze/mixed-tests/20261018T205950Z-2afc5252/clean_large.csv`
- Broken: `bronze/mixed-tests/20261018T205950Z-2afc5252/late_invalid_amount.csv`

Teammates share one login and the same incident database as the local app. There
is no individual attribution or role separation. New cloud outputs use
`silver/shared-demo/` and `quarantine/shared-demo/`. The server cannot modify
Bronze. Jobs queue one at a time. SNS email delivery is disabled. The remediation
sensor starts stopped; enabling it processes pending incidents in the shared DB.
S3 arrivals do not automatically launch validation jobs.

The URL uses a reserved Elastic IP, free `sslip.io` DNS, and Caddy's automatically
renewed public TLS certificate. Free DNS is a demo dependency; use a domain you
own for a long-lived deployment. HTTP redirects to HTTPS, application ports are
private to the server, and administration uses Systems Manager instead of SSH.

## Collaborate and deploy changes

Share your repository with teammates. They clone it, create branches, edit code,
and submit pull requests. Browser access provides experiments and incident review;
code editing happens in their editor and repository. An AWS-authorized maintainer
deploys a reviewed checkout:

```powershell
git pull
python -m pytest tests -q
python scripts/deploy_team.py update --profile default
```

The command uploads a small source archive to private S3 and installs it through
Systems Manager. The explicit source allowlist excludes `.env*`, AWS credentials,
Git files, datasets, logs, and `friend-agentic-aws-dataops/`. Updates refuse to
proceed while a Dagster run is active. Coordinate a brief maintenance window:
services restart during updates. Pushing to Git alone does not deploy.

The previous source release stays on the server; its path is saved in
`/opt/dataops/previous-release`. Dependencies share a Python environment, so a
source rollback alone does not undo dependency changes. Infrastructure and
dependency changes need separate review. Browser users do not need AWS keys;
never share `.env.aws.local` or your personal AWS credentials.

## Initial setup

An AWS profile needs permission to create this CloudFormation stack and its EC2,
IAM, and Secrets Manager resources. The default bucket/database are this project's
existing resources in `us-east-1`. For another account, supply `--bucket`,
`--database`, `--region`, and `--profile`.

```powershell
python scripts/deploy_team.py package
python scripts/deploy_team.py deploy --profile default
```

Review [the template](../deploy/aws/template.yaml). It adds a server, encrypted
40 GB disk, reserved IP, login secret, server role, and RDS ingress limited to
the new server security group. It reuses S3 and RDS. Use `deploy` only for initial
creation and `update` for later source changes. Installation and the model download
happen on AWS and can take several minutes after stack creation.

```powershell
uvx --from cfn-lint cfn-lint deploy/aws/template.yaml --regions us-east-1
aws cloudformation validate-template --region us-east-1 --template-body file://deploy/aws/template.yaml
```

The runtime policy baseline came from IAM Policy Autopilot, scoped to the relevant
bucket prefixes, two secrets, and Nova Micro. Static AWS keys and SNS publishing
are omitted from the server.

## Cost and maintenance

AWS Pricing returned **$0.0752/hour** for Linux `t3a.large` in `us-east-1` on
2026-10-19: approximately **$54.90 for 730 hours**, before credits or tax. Disk,
reserved IPv4, Secrets Manager, transfer, S3 operations, and Bedrock are additional.
Existing RDS/S3 charges continue. Standard CPU credits avoid surplus-credit bills;
sustained CPU work can slow down after credits run out.

The 2B Decider converts its weights to float32 on CPU (approximately 7 GiB for
the model alone). On this 8 GiB server it uses swap alongside the web services,
so AI diagnosis is slow and can reach the configured 600-second timeout. The
data-quality jobs and dashboard have been verified on this size. Keep automatic
remediation stopped unless you accept that limitation; a 16 GiB server provides
more room for the Decider but increases the hourly charge.
The deployment's simple Decider check succeeded but took about seven minutes.

```powershell
python scripts/deploy_team.py stop
python scripts/deploy_team.py start
```

Stopping pauses compute charges; disk and reserved IP charges continue. The URL
stays the same after restarting. To remove hosting, review and delete the
`agentic-dataops-team` CloudFormation stack. Existing S3 and RDS are outside that
stack. The root disk is deliberately retained to preserve run history: inspect
and separately delete it when no longer needed. Source archives remain in the
bucket under `deployments/team/`.

Use Systems Manager Session Manager for the instance printed by `status`:

```bash
sudo cloud-init status --long
sudo tail -n 50 /var/log/cloud-init-output.log
sudo systemctl status dataops-web dataops-daemon dataops-dashboard caddy
sudo journalctl -u dataops-web -u dataops-daemon -u dataops-dashboard --since '15 minutes ago'
sudo journalctl -u caddy --since '15 minutes ago'
```

Do not paste `/etc/dataops/runtime.env`, `/etc/dataops/application.env`, or the
team login into issues. Settings are generated from Secrets Manager during setup;
regenerate them and restart services after a database password rotation.
