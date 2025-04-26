# Dockerfile
#
# $> docker build -t cdc-repo-scanner .
# $> docker run --rm --env-file .env -v "$(pwd):/app" cdc-repo-scanner
# $> docker run --rm --env-file .env -e GITLAB_SSL_VERIFY=false -v "$(pwd):/app" cdc-repo-scanner
##
# 1. Base Image
FROM python:3.11-slim

# 2. Set Working Directory
WORKDIR /app

# --- Add CA Certificate ---
# Create the directory for extra CA certificates
# Copy your custom CA certificate file into the container
RUN mkdir -p /usr/local/share/ca-certificates/

# (Optional) Set the environment variable for requests
#ENV REQUESTS_CA_BUNDLE=/app/my-corp-ca.crt

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
# Handle certificates
COPY ./zscaler/CDC-CSPO-PA.crt /usr/local/share/ca-certificates/
COPY ./zscaler/ZScalerRootCA.crt /usr/local/share/ca-certificates/
COPY ./zscaler/CloudflareR2.crt /usr/local/share/ca-certificates/
RUN chmod -R 755 /etc/ssl/certs
# Update the container's certificate store to include the new certificate
RUN update-ca-certificates
# --- End CA Certificate Addition ---

# 3. Copy Requirements First
COPY requirements.txt ./

# 4. Install Dependencies
RUN pip install --no-cache-dir --trusted-host pypi.python.org --trusted-host pypi.org -r requirements.txt

# 5. Copy Application Code
COPY . .

# 6. Command to Run
CMD ["python", "generate_codejson.py"]
