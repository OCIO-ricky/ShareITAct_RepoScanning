#!/bin/bash

# Optional way to run the scanner on multiple platforms repos concurrently using docker.
#
# Docker image name - ensure this image is built and available.
# You can build it using the Dockerfile in your project root, e.g.:
# docker build -t cdc-repo-scanner:latest .
IMAGE_NAME="cdc-repo-scanner:latest"

echo "---"
echo "IMPORTANT SECURITY NOTE:"
echo "This script can use hardcoded tokens as a fallback (from your docker-compose.yml)."
echo "For better security, please set YOUR_GITHUB_PAT, YOUR_GITLAB_PAT, YOUR_AZURE_DEVOPS_PAT, and YOUR_GOOGLE_API_KEY"
echo "as environment variables in your shell before running this script."
echo "Example: export YOUR_GITHUB_PAT=\"ghp_your_real_token\""
echo "---"
echo ""
echo "NOTE: This script assumes that the necessary output directories (e.g., ./output  and the ./logs directory"
echo "already exist in the project root where this script is executed."
echo ""

# --- GitHub Scan ---
echo "Starting GitHub scan..."
docker run -d --rm \
  --name scanner-github-instance-bash \
  -e GITHUB_TOKEN=<YOUR_GITHUB_PAT> \          #  <== replace <..> with the actual token key for hardcoded. 
  -e GOOGLE_API_KEY="${YOUR_GOOGLE_API_KEY}" \   #  <== instead of hardcoded, take it from .env file
  -e GITHUB_ORGS="CDCent,CDCgov" \
  -e SCANNER_MAX_WORKERS="3" \
  -e AI_ENABLED="true" \
  -v "$(pwd)/output_github:/app/output" \
  -v "$(pwd)/logs:/app/logs" \
  -v "$(pwd)/.env:/app/.env:ro" \
  "$IMAGE_NAME" \
  python generate_codejson.py --platform github

# --- GitLab Scan ---
echo "Starting GitLab scan..."
docker run -d --rm \
  --name scanner-gitlab-instance-bash \
  -e GITLAB_TOKEN="${YOUR_GITLAB_PAT}" \
  -e GOOGLE_API_KEY="${YOUR_GOOGLE_API_KEY}" \
  -e GITLAB_GROUPS="cdcent" \
  -e SCANNER_MAX_WORKERS="4" \
  -e AI_ENABLED="${AI_ENABLED_FOR_GITLAB:-true}" \
  -v "$(pwd)/output_gitlab:/app/output" \
  -v "$(pwd)/logs:/app/logs" \
  -v "$(pwd)/.env:/app/.env:ro" \
  "$IMAGE_NAME" \
  python generate_codejson.py --platform gitlab

# --- Azure DevOps Scan (Example - Uncomment and configure if needed) ---
echo "Starting Azure DevOps scan..."
docker run -d --rm \
  --name scanner-azure-instance-bash \
  -e AZURE_DEVOPS_TOKEN="${YOUR_AZURE_DEVOPS_PAT}" \
  -e GOOGLE_API_KEY="${YOUR_GOOGLE_API_KEY}" \
  -e AZURE_DEVOPS_TARGETS="MyAzureOrg/ProjectA,MyAzureOrg/ProjectB" \
  -e SCANNER_MAX_WORKERS="4" \
  -e AI_ENABLED="${AI_ENABLED_FOR_AZURE:-true}" \
  -v "$(pwd)/output_azure:/app/output" \
  -v "$(pwd)/logs:/app/logs" \
  -v "$(pwd)/.env:/app/.env:ro" \
  "$IMAGE_NAME" \
  python generate_codejson.py --platform azure

echo ""
echo "All scans launched. You can monitor them with 'docker ps'."
echo "To check logs for a specific scan, use 'docker logs <container_name>', e.g.:"
echo "  docker logs scanner-github-instance-bash"
echo "  docker logs scanner-gitlab-instance-bash"
echo ""
echo "Individual scan outputs will be in ./output
echo "Shared logs will be in ./logs and also in ./output/logs"
echo "The .env file from your host is mounted read-only at /app/.env in each container."

