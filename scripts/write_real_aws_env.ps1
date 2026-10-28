$ErrorActionPreference = "Stop"

$region = "us-east-1"
$secretArn = "arn:aws:secretsmanager:us-east-1:731451715213:secret:rds!db-ff8168f6-a84d-4bad-b448-3f3a6008501c-BHnCtK"
$dbHost = "agentic-dataops-postgres.c8zes4ouet7m.us-east-1.rds.amazonaws.com"
$dbName = "dataops"
$bucketName = "agentic-dataops-731451715213-us-east-1"
$outputPath = Join-Path $PSScriptRoot "..\.env.aws.local"

$secretJson = aws secretsmanager get-secret-value `
  --region $region `
  --secret-id $secretArn `
  --query SecretString `
  --output text

$secret = $secretJson | ConvertFrom-Json
$databaseUrl = "postgresql+psycopg://$($secret.username):$($secret.password)@$dbHost`:5432/$dbName"

$content = @"
DATABASE_URL=$databaseUrl
AWS_REGION=$region
AWS_PROFILE=default

S3_BUCKET_NAME=$bucketName
S3_BRONZE_PREFIX=bronze/
S3_SILVER_PREFIX=silver/
S3_QUARANTINE_PREFIX=quarantine/

STRANDS_DECIDER_ENDPOINT=
STRANDS_DECIDER_API_KEY=
DECIDER_MODE=cli
STRANDS_DECIDER_MODEL_ID=StrandsAgents/strands-decider-2B-hobson-v19

BEDROCK_MODEL_ID=amazon.nova-micro-v1:0
ALERT_SNS_TOPIC_ARN=arn:aws:sns:us-east-1:731451715213:agentic-dataops-engineer-alerts
MAX_NULL_PERCENTAGE=0.15
MAX_RETRY_COUNT=2
"@

Set-Content -LiteralPath $outputPath -Value $content -Encoding ASCII
Write-Host "Wrote $outputPath"
Write-Host "Do not commit this file. It contains the RDS password."
