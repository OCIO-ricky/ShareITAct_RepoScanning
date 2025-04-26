# CDC Repository Metadata Scanner (code.json Generator)

This script scans repositories across GitHub, GitLab, and Azure DevOps for a configured organization/group, extracts metadata according to specific rules, applies exemption logic, handles private repositories, and generates a consolidated `code.json` file compliant with the Code.gov schema v2.0.

## Features

-   Connects to GitHub, GitLab, and Azure DevOps APIs.
-   Extracts metadata like description, license, dates, languages, etc.
-   Parses README files for specific metadata (`Org:`, `Contract#:`, manual exemptions).
-   Applies a cascade of exemption logic (Manual, Non-code, Keywords).
-   Handles private repositories:
    -   Assigns unique `privateID`.
    -   Maps organization and contact details based on README.
    -   Uses specific contact email (`shareit@cdc.gov`) if `Email Requests:` is found.
-   Logs exempted repositories to a CSV file.
-   Manages and persists `privateID` mappings in a CSV file.
-   Generates a single `code.json` file containing metadata for *all* scanned repositories.

## Setup

1.  **Clone the repository:**
    ```bash
    git clone <your-repo-url>
    cd <your-repo-directory>
    ```

2.  **Create a virtual environment (recommended):**
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows use `venv\Scripts\activate`
    ```

3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Configure Environment Variables:**
    -   Copy `.env.example` to `.env` (or create `.env` manually).
    -   Fill in the required API tokens and organization/group names in the `.env` file:
        -   `GITHUB_TOKEN`, `GITHUB_ORG`
        -   `GITLAB_TOKEN`, `GITLAB_GROUP`
        -   `AZURE_DEVOPS_TOKEN`, `AZURE_DEVOPS_ORG`, `AZURE_DEVOPS_PROJECT` (optional)
        -   You can also customize `ExemptedCSVFile`, `PrivateIDCSVFile`, `DEFAULT_CONTACT_EMAIL`, `PRIVATE_REPO_CONTACT_EMAIL`.
    -   **Important:** Add `.env` to your `.gitignore` file to prevent committing secrets.

## Running the Script

Execute the main script from the project's root directory:

```bash
python generate_codejson.py
```
## Running with Docker

This project includes a `Dockerfile` to allow building and running the application within a containerized environment. This ensures consistency across different systems.

### Prerequisites

*   Docker installed and running on your system.
*   A `.env` file created in the project root directory containing the necessary API tokens and configuration (see **Configuration** section above).

### Building the Docker Image

1.  **Navigate** to the root directory of the project (where the `Dockerfile` is located) in your terminal.
2.  **Build the image** using the following command. Replace `cdc-repo-scanner` with your desired image name and tag:

    ```bash
    docker build -t cdc-repo-scanner .
    ```

    *   If you encounter issues with cached layers (e.g., after updating dependencies or certificates), you might need to build without the cache:
        ```bash
        docker build --no-cache -t cdc-repo-scanner .
        ```

### Running the Docker Container

Once the image is built, you can run the scanner using the `docker run` command. It's crucial to provide the environment variables from your `.env` file and map the output directory so the generated files (`code.json`, logs, CSVs) persist on your host machine.

1.  **Ensure your `.env` file** is present in the project root directory on your host machine.
2.  **Create an `output` directory** in the project root on your host machine if it doesn't exist. This is where the container will write its results.

    ```bash
    mkdir output
    ```

3.  **Run the container:**

    ```bash
    docker run --rm \
      --env-file .env \
      -v "$(pwd)/output:/app/output" \
      cdc-repo-scanner
    ```

    **Explanation of options:**
    *   `--rm`: Automatically removes the container when it exits.
    *   `--env-file .env`: Loads environment variables from the `.env` file in your current host directory into the container.
    *   `-v "$(pwd)/output:/app/output"`: Mounts the `output` directory from your current host directory into the `/app/output` directory inside the container. This allows the container to write results back to your host.
    *   `cdc-repo-scanner`: The name of the image you built.

4.  **(Optional) Overriding Environment Variables:** If you need to override specific variables from the `.env` file or pass variables not included in it (like proxy settings or SSL flags), you can use the `-e` flag:

    ```bash
    docker run --rm \
      --env-file .env \
      -e HTTPS_PROXY=http://your-proxy-server:port \
      -e HTTP_PROXY=http://your-proxy-server:port \
      -e GITLAB_SSL_VERIFY=false \
      -v "$(pwd)/output:/app/output" \
      cdc-repo-scanner
    ```

### Accessing Results

After the container finishes running, the generated `code.json`, `exempted_log.csv`, `privateid_mapping.csv`, and log files will be available in the `output` directory on your host machine.

