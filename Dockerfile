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
# IMPORTANT: Place your CA certificate file (e.g., "my-corp-ca.crt")
# in the same directory as this Dockerfile on your host machine.
# RUN mkdir -p /usr/local/share/ca-certificates/
# COPY my-corp-ca.crt /usr/local/share/ca-certificates/my-corp-ca.crt

# (Optional) Set the environment variable for requests
#ENV REQUESTS_CA_BUNDLE=/app/my-corp-ca.crt

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
