# Dockerfile:
# This Dockerfile is used to build a Docker image for the CDC Repo Scanner application.
# You can run the whole thing in a container, which is useful for testing and deployment.
#
# $> docker-compose up --build -d
#
# That does it !! 
#
# You can also run the container directly using Docker commands:
# $> docker build -t cdc-repo-scanner .
# $> docker run --rm --env-file .env -v "$(pwd)/output:/app/output" -v "$(pwd)/logs:/app/logs" cdc-repo-scanner
# or
# $> docker run --rm --env-file .env -e GITLAB_SSL_VERIFY=false -v "$(pwd)/output:/app/output" -v "$(pwd)/logs:/app/logs" cdc-repo-scanner
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

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        build-essential \
        libffi-dev \
    && rm -rf /var/lib/apt/lists/*

# Handle corporate certificates
# Copy all .crt and .pem certificate files from the ./zscaler directory on the host.
# Ensure your corporate certificates have .crt or .pem extensions.
COPY ./zscaler/*.crt /usr/local/share/ca-certificates/
COPY ./zscaler/*.pem /usr/local/share/ca-certificates/

RUN chmod -R 755 /etc/ssl/certs
# Update the container's certificate store to include the new certificate
RUN update-ca-certificates
# --- End CA Certificate Addition ---

# 3. Copy Requirements First
COPY requirements.txt ./

# 4. Install Dependencies
# Upgrade pip, setuptools, and wheel to ensure they are up-to-date
RUN pip install --no-cache-dir --upgrade pip setuptools wheel
# The '|| true' ensures the command doesn't fail if a package isn't already installed.
RUN pip uninstall -y msrest azure-core azure-devops azure-identity python-gitlab || true && \
    pip install --no-cache-dir --trusted-host pypi.python.org --trusted-host pypi.org -r requirements.txt

# 5. Copy Application Code
COPY . .

# 6. Command to Run
CMD ["bash"]   
