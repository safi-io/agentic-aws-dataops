#!/usr/bin/env bash
# Runs on the AWS instance, never on a teammate's PC.
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y python3-venv caddy curl
id dataops >/dev/null 2>&1 || useradd --system --create-home --home-dir /var/lib/dataops --shell /bin/bash dataops
install -d -o dataops -g dataops /var/lib/dataops/dagster /var/lib/dataops/huggingface
install -d -m 0750 -o root -g dataops /etc/dataops
if [ ! -f /swapfile ]; then
    fallocate -l 4G /swapfile
    chmod 600 /swapfile
    mkswap /swapfile
    swapon /swapfile
    echo '/swapfile none swap sw 0 0' >> /etc/fstab
fi
python3 -m venv /opt/dataops/venv
/opt/dataops/venv/bin/pip install --no-cache-dir --upgrade pip
# CPU wheels avoid downloading CUDA libraries to a CPU-only server.
/opt/dataops/venv/bin/pip install --no-cache-dir torch==2.9.1 --index-url https://download.pytorch.org/whl/cpu
/opt/dataops/venv/bin/pip install --no-cache-dir -r /opt/dataops/app/deploy/aws/requirements.txt
/opt/dataops/venv/bin/python /opt/dataops/app/deploy/aws/configure_server.py
cp /opt/dataops/app/deploy/aws/dataops-*.service /etc/systemd/system/
chown -R dataops:dataops /var/lib/dataops
systemctl daemon-reload
systemctl enable --now dataops-web dataops-daemon dataops-dashboard
systemctl restart caddy
echo 'DataOps web services started. Checking the model cache on AWS.'
runuser -u dataops -- env HF_HOME=/var/lib/dataops/huggingface /opt/dataops/venv/bin/python -c \
  "from huggingface_hub import snapshot_download; snapshot_download('StrandsAgents/strands-decider-2B-hobson-v19', allow_patterns=['*.json','*.safetensors','*.txt','*.model','*.jinja'])"
touch /var/lib/dataops/bootstrap-complete
