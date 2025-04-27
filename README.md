## Introduction

This tool was developed in response to the Strengthening Homeland and Organizational Resilience through Empowering Innovative Technologies (SHARE IT) Act, which requires federal agencies to inventory their software assets and make appropriate code available for reuse across government or as open source. 

The SHARE IT Act mandates that federal agencies:
1. Create and maintain a comprehensive inventory of custom-developed code
2. Publish this inventory in a standardized code.json format
3. Release appropriate software projects as open source or for government-wide reuse
4. Properly document exemptions when code cannot be shared due to security, privacy, or other valid concerns

This application automates the process of scanning repositories across multiple platforms (GitHub, GitLab, Azure DevOps), analyzing their content, determining appropriate sharing status, and generating a compliant code.json file. It uses AI-powered analysis to identify organization ownership, determine appropriate exemption codes when needed, and extract relevant metadata to ensure compliance with federal requirements.

By automating these processes, the tool helps agencies efficiently meet their SHARE IT Act obligations while ensuring sensitive code remains properly protected through appropriate exemption categorization.

## Features

- **Multi-Platform Support**: Scans repositories across GitHub, GitLab, and Azure DevOps platforms
- **AI-Powered Analysis**: Uses Google's Generative AI to:
  - Identify organization names from repository content
  - Determine appropriate exemption codes based on repository content
  - Provide justifications for exemptions
- **Automated Exemption Processing**:
  - Detects non-code repositories and applies appropriate exemptions
  - Identifies sensitive content through keyword scanning
  - Applies manual exemptions from README files
  - Uses AI analysis as a fallback when other methods don't apply
- **Contact Information Extraction**:
  - Extracts contact emails from README and CODEOWNERS files
  - Applies appropriate contact information based on repository visibility
- **Metadata Enhancement**:
  - Extracts version information from README content
  - Parses tags/keywords from repository documentation
  - Attempts to identify license information
- **Private Repository Handling**:
  - Generates secure private IDs for non-public repositories
  - Applies appropriate default contact information for private repos
  - Extracts organization and contract information from private repos
- **Comprehensive Logging**:
  - Detailed logging of processing steps and decisions
  - Error handling with informative messages
- **Configurable Environment**:
  - Uses environment variables for customization
  - Supports rate limiting for API calls
  - Configurable AI processing parameters
- **Code.json Generation**:
  - Produces compliant code.json output following federal guidelines
  - Properly formats repository metadata according to schema requirements

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

```bash
docker run --rm --env-file .env -v "$(pwd):/app" cdc-repo-scanner
```

 

### Accessing Results

After the container finishes running, the generated `code.json`, `exempted_log.csv`, `privateid_mapping.csv`, and log files will be available in the `output` directory on your host machine.

