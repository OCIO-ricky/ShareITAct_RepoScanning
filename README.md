## Introduction

This tool was developed in response to the Strengthening Homeland and Organizational Resilience through Empowering Innovative Technologies (SHARE IT) Act, which requires federal agencies to inventory their software assets and make appropriate code available for reuse across government or as open source. 

The SHARE IT Act mandates that federal agencies:
1. **Inventory Custom Code**: Require federal agencies to create and maintain inventories of their custom-developed software.
2. **Enable Government-Wide Reuse**: Require agencies to acquire the necessary rights and make their new custom-developed code available for reuse across other federal agencies, unless a specific exception applies. This means agencies must ensure their contracts or development processes give them the legal permission (intellectual property rights/licenses) to share the code with other agencies.
3. **Pilot Open Source Release**: Establish a requirement for agencies to release at least 20% of their newly developed custom code to the public as Open Source Software (OSS).
4. **Document Exceptions**: Require agencies to formally document the justification when custom code cannot be shared for reuse or released as OSS due to specific, predefined exceptions (like security risks, privacy concerns, national security, etc.).

The existing OMB M-16-21 Federal Source Code Policy, which aims to enhance government efficiency and innovation through software reuse and open source practices, already has specifications that align well with the goals outlined in the proposed SHARE IT Act. Following this approach, the CDC team chose to adopt the `code.json` schema version 2.0 specification to publish the software inventory. This application directly supports that decision by automating the production of the compliant `code.json` file, ensuring the inventory captures custom-developed code residing in **both public and private repositories**. It achieves this by scanning repositories across multiple platforms (GitHub, GitLab, Azure DevOps), analyzing content to determine appropriate sharing status, and utilizing AI-powered analysis to identify organization ownership, assign necessary exemption codes, and extract the relevant metadata required by the schema.

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
    git clone https://github.com/OCIO-ricky/ShareITAct_RepoScanning.git
    cd ShareITAct_RepoScanning
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

---
## ✨ *Setup with Docker* ✨
---


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

