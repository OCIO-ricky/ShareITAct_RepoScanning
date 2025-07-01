# 🏛️ Share IT Act – Repository Scanning Tool

<!-- Add your badges here -->
<p align="center">
  <a href="https://github.com/OCIO-ricky/ShareITAct_RepoScanning/actions/workflows/python-app.yml"><img alt="Build Status" src="https://img.shields.io/github/actions/workflow/status/OCIO-ricky/ShareITAct_RepoScanning/python-app.yml?branch=main&style=for-the-badge&logo=githubactions"></a>
  <a href="https://www.python.org/"><img alt="Python Version" src="https://img.shields.io/badge/python-3.9%2B-blue?style=for-the-badge&logo=python"></a>
  <a href="https://github.com/OCIO-ricky/ShareITAct_RepoScanning/blob/main/LICENSE.md"><img alt="License" src="https://img.shields.io/github/license/OCIO-ricky/ShareITAct_RepoScanning?style=for-the-badge"></a>
</p>

## 👋 Introduction

Welcome to the **Share IT Act – Repository Scanning Tool**! This utility is engineered to streamline compliance with the SHARE IT Act (Public Law 118-187) for CDC organizations. It automatically scans code repositories across **GitHub, GitLab, and Azure DevOps**, generating a `code.json` metadata file that is fully compliant with the code.gov schema v2.0. This process ensures an accurate and machine-readable inventory.
Leveraging **AI-driven insights** for inferring code sharing exemptions and offering a **portable design**, the tool helps facilitate inventory efforts across the many mission units across CDC.

For comprehensive business-side documentation and process details, please refer to the internal [CDC's EA SHare IT Act](https://cdc.sharepoint.com/sites/EITPO/EA/SitePages/Enterprise-Architecture.aspx). 

---
## ✨ Features

This tool directly supports compliance with the SHARE IT Act by:

-   **Automated Inventory Creation:** Scans repositories across **GitHub, GitLab, and Azure DevOps (ADO)** to automatically gather metadata.  Other than ADO, graphQL is used to more efficiently (faster) query these repositories and to reduce the number of API calls and associated rate limit restrictions. Unfortunately, ADO does not support graphQL and so its REST API is still used instead.
-   **Compliant Metadata Generation:** Produces `code.json` files that are validated against the **code.gov schema v2.0**, meeting federal requirements. 
    > **Note on License Detection:** For GitHub and GitLab, the tool leverages the platform's built-in license detection. For Azure DevOps, which does not provide this feature via its API, the tool manually searches for and verifies the existence of a license file within the repository.
    > **Note on README Parsing:** For all platforms, including Azure DevOps, the tool fetches and parses the `README.md` file. This is critical for detecting manual exemption markers, inferring metadata (like version or organization), and providing content for AI-driven analysis.
-   **Exemption Assistance:** Utilizes AI-driven insights to infer potential code sharing exemptions based on repository content (like `README.md`) and metadata.
-   **Comprehensive Data Collection:** Extracts detailed information including project descriptions, languages, licenses, and estimates labor hours from commit history.
-   **Centralized Reporting:** Consolidates all generated `code.json` files, exemption logs, and private ID mappings into a structured output for easy review and agency-wide reporting.
-   **Flexible Deployment:** Offers execution via Docker (recommended for concurrent scans and consistency) or as a standalone CLI tool.

Additional capabilities include:
-   Suggestion of organization or office names to aid in `code.json` metadata accuracy.
-   Detection of existing exemption flags and classifications within repositories.
-   Support for scanning both public and private repositories.

## 🐳 Getting Started with Docker (Recommended)

Using Docker (specifically `docker-compose`) is the preferred method for running the Repository Scanning Tool. It simplifies dependency management, ensures a consistent environment, and allows for efficient concurrent scanning of multiple platforms.

### Prerequisites

1.  **Docker and Docker Compose:** Ensure Docker and Docker Compose are installed on your system.
2.  **Clone the Repository:**
    ```bash
    git clone https://github.com/OCIO-ricky/ShareITAct_RepoScanning.git
    cd ShareITAct_RepoScanning
    ```
3.  **Configure Environment Variables (`.env` file):**
    This is a crucial step. API tokens and target configurations are managed via an `.env` file.
    ```bash
    cp docs/.env.template .env
    ```
    Now, **edit the `.env` file** in the project root directory. 
    
     ```bash
    # -------------------------------
    #  AUTHENTICATION TOKENS
    # -------------------------------
    # --- Authentication Tokens & API Keys (IMPORTANT: Keep these secure!) ---
    GITHUB_TOKEN="YOUR_GITHUB_PAT" # Personal Access Token for GitHub
    GITLAB_TOKEN="YOUR_GITLAB_PAT" # Personal Access Token for GitLab
    AZURE_DEVOPS_TOKEN="YOUR_AZURE_DEVOPS_PAT" # Personal Access Token for Azure DevOps
    GOOGLE_API_KEY=YOUR_GOOGLE_API_KEY # For AI features (Gemini)
    ```
    Populate it with your API tokens (GitHub, GitLab, Azure DevOps), target organizations/groups/projects, and any other necessary configurations (e.g., `GOOGLE_API_KEY` for AI features).
    **Important:** The .gitignore file will NOT commit the `.env` file to GitHub with your actual secrets.
    ```bash
    # -------------------------------
    #  TARGET ORGANIZATIONS/GROUPS/PROJECTS
    # -------------------------------
    # ---
    GITHUB_ORGS=CDCent,CDCgov,informaticslab,cdcai,epi-info,niosh-mining

    GITLAB_URL=https://gitlab.com
    GITLAB_GROUPS=group1, group2, group3

    AZURE_DEVOPS_API_URL=https://dev.azure.com
    AZURE_DEVOPS_ORG=MyAzureOrg # Default Azure DevOps organization name
    AZURE_DEVOPS_TARGETS=MyAzureOrg/ProjectA,MyAzureOrg/ProjectB,AnotherOrg/ProjectC # OrgName/ProjectName pairs
    ```

### Option 1: Running All Configured Scans Concurrently

This is ideal for a full inventory run, scanning all platforms you've configured in your `.env` file and/or `docker-compose.yml`. The `docker-compose.yml` file is set up to run scans for GitHub, GitLab, and Azure DevOps in parallel and merge the results producing a single `code.json` file.

**Start the scan services:**
```bash
docker-compose up --build -d
```
-   `--build`: Builds the Docker image if it doesn't exist or if `Dockerfile` has changed.
-   `-d`: Runs the containers in detached mode (in the background).

This will start separate containers for each platform defined as a service in `docker-compose.yml` (e.g., `scan-github`, `scan-gitlab`, `scan-azure`, and `merge-results`). Each will execute its respective scan command.


### Option 2: Running a Specific Scan or Command

If you want to run a scan for a single platform, or execute a specific command like `merge` without running all concurrent scans first.

1.  **Execute the desired command:**
    Use `docker-compose run --rm app` followed by the `python generate_codejson.py` command and its arguments.
    ```bash
    # Example: Scan only specific GitHub organizations
   docker-compose run --rm --no-deps scan-github  python generate_codejson.py github --gh-tk YOUR_GITHUB_PAT --orgs YourOrg1,YourOrg2

    # Example: Scan only specific GitLab groups
    docker-compose run --rm --no-deps scan-gitlab python generate_codejson.py gitlab --gl-tk YOUR_GITLAB_PAT --groups your-group

    # Example: Run only the merge command
    docker-compose run --rm --no-deps merge-results python generate_codejson.py merge
    ```
    Remember to replace placeholders like `YOUR_GITHUB_PAT` with actual values or ensure they are correctly set in your `.env` file (which `docker-compose` will typically load).

### Dealing with SSL Certificate Verification Errors (Corporate Networks)

> [!IMPORTANT]
> If you are running Docker containers within a corporate network (e.g., CDC's network) that uses its own SSL certificates for security inspection or for internally hosted services, you might encounter `CERTIFICATE_VERIFY_FAILED` errors during the scanner run. This typically happens when the tool, running inside a Docker container, tries to connect to external services like GitHub, GitLab, or Azure DevOps. The corporate network's security appliances (often a proxy, like Zscaler) intercept this traffic, decrypt it, examine it, and then re-encrypt it before sending it on to your Docker container.  The re-encryption uses a corporate certificate that Docker may not know about. This causes the Docker container to fail with the `CERTIFICATE_VERIFY_FAILED` error. 

To resolve this, simply ensure your corporate CA certificates (with .crt or .pem extensions) are placed in the ./zscaler directory of this project.
Alternatively, if managing corporate certificates is complex or not feasible, consider running the Docker container on a cloud service (e.g., AWS, Azure, GCP). This approach typically bypasses corporate network SSL inspection issues.
#### Detailed Steps:
1.  **Obtain Corporate CA Certificate(s):**
    *   **Crucial Step:** Contact your IT department (e.g., CDC IT/Network Security) to obtain the necessary root CA certificate(s) and any intermediate CA certificates. These are essential for your applications to trust connections made from within the corporate network.
    *   These certificates are usually in `.pem` or `.crt` format.

2.  **Place Certificates in the `./zscaler` Directory:**
    *   In the root directory of this project (where `Dockerfile` and `docker-compose.yml` are located), create a directory named `zscaler` if it doesn't already exist.
    *   Place all the corporate CA certificate files provided by your IT department into this `./zscaler` directory. For example, if IT gives you `CDC.crt` and `CDC.pem`, put them in `./zscaler/`.
3. **Start the scan services:**
    ```bash
    docker-compose up --build -d
    ```

### Managing Docker Containers

-   **View Logs:**
    To see the output/logs of all running services:
    ```bash
    docker-compose logs -f
    ```
    To follow logs for a specific service (e.g., `scan-github`):
    ```bash
    docker-compose logs -f scan-github
    ```
-   **Stop Services:**
    To stop the running services (if started with `-d`):
    ```bash
    docker-compose stop
    ```
-   **Stop and Remove Services/Networks/Volumes:**
    To stop services and remove containers, networks, and (optionally) volumes:
    ```bash
    docker-compose down
    ```

## 🛠️ Manual Installation & CLI Usage (Alternative)

If you prefer not to use Docker, or if you are contributing to the development of the tool, you can set it up and run it directly on your machine.

### Prerequisites

-   Python (version specified in development, e.g., 3.9+)
-   `pip` and `venv`

### Setup

1.  **Clone the repository (if not already done):**
    ```bash
    git clone https://github.com/OCIO-ricky/ShareITAct_RepoScanning.git
    cd ShareITAct_RepoScanning
    ```
2.  **Create and activate a virtual environment:**
    ```bash
    python -m venv venv
    # On Windows:
    venv\Scripts\activate
    # On macOS/Linux:
    source venv/bin/activate
    ```
3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```
4.  **Configure Environment Variables (`.env` file):**
    Copy the `docs/.env.template` file to `.env` in the project root directory.
    ```bash
    cp docs/.env.template .env
    ```
    Then, edit the `.env` file and populate it with your specific settings and credentials (API tokens, target orgs/groups, `GOOGLE_API_KEY`, etc.). **Do not commit the `.env` file with your actual secrets to version control.**

### Running the Tool (CLI)

All commands should be run from the root directory of the project where `generate_codejson.py` is located, with your virtual environment activated.

#### General Usage

```bash
python generate_codejson.py <command> [options]
```
### Available Commands

1.  **`github`**: Scan GitHub organizations.
    *   **Authentication:**
        *   `--gh-tk <YOUR_GITHUB_PAT>`: **Required.** Your GitHub Personal Access Token.
    *   **Targeting Public GitHub.com:**
        *   `--orgs <org1,org2>`: Comma-separated public GitHub.com organizations to scan. If not provided, uses `GITHUB_ORGS` from `.env`.
        *   `--limit <number>` **optional** Limit the number of repositories to scan per organization. Useful for testing.
        ```bash
        python generate_codejson.py github --gh-tk YOUR_GITHUB_PAT --orgs YourOrg1,YourOrg2 --limit 10
        ```
    *   **Targeting GitHub Enterprise Server (GHES):**
        *   `--github-ghes-url <GHES_URL>`: **Required.** The URL of your GHES instance (e.g., `https://github.mycompany.com`).
        *   `--orgs <ghes_org1,ghes_org2>`: **Required.** Comma-separated organizations on the specified GHES instance.
        ```bash
        python generate_codejson.py github --gh-tk YOUR_GITHUB_PAT --github-ghes-url https://github.mycompany.com --orgs YourGHESOrg1 --limit 5
        ```
    *   *Output: Generates `intermediate_github_<OrgName>.json` files.*


2.  **`gitlab`**: Scan GitLab groups.
    *   **Authentication:**
        *   `--gl-tk <YOUR_GITLAB_PAT>`: **Required.** Your GitLab Personal Access Token.
    *   **Targeting:**
        *   `--groups <group1/subgroup,group2>`: Comma-separated GitLab group paths. If not provided, uses `GITLAB_GROUPS` from `.env`.
        *   `--gitlab-url <GITLAB_INSTANCE_URL>`: URL of your GitLab instance (e.g., `https://gitlab.mycompany.com`). If not provided, uses `GITLAB_URL` from `.env` (which defaults to `https://gitlab.com`).
        *   `--limit <number>` **optional** Limit the number of repositories to scan per organization for debugging purposes.
        ```bash
        python generate_codejson.py gitlab --gl-tk YOUR_GITLAB_PAT --gitlab-url https://git.biotech.cdc.gov --groups my-group/subgroup --limit 10
        ```
 *   *Output: Generates `intermediate_gitlab_<GroupPath>.json` files.*

3.  **`azure`**: Scan Azure DevOps organization/project targets.
    *   **Authentication (choose one method):**
        *   **PAT Token:**
            *   `--az-tk <YOUR_AZURE_PAT>`: Your Azure DevOps Personal Access Token.
        *   or **Service Principal:**
            *   `--az-cid <CLIENT_ID>`: Service Principal Client ID.
            *   `--az-cs <CLIENT_SECRET>`: Service Principal Client Secret.
            *   `--az-tid <TENANT_ID>`: Service Principal Tenant ID.
    *   **Targeting:**
        *   `--targets <Org1/ProjA,Org2/ProjB>`: Comma-separated Azure DevOps `OrganizationName/ProjectName` pairs. If not provided, uses `AZURE_DEVOPS_TARGETS` from `.env`.
        *   `--limit <number>` **optional** Limit the number of repositories to scan per organization.
        *   *Note: The environment variable `AZURE_DEVOPS_API_URL` defaults to https://dev.azure.com and is sourced from `.env` *
        *    The environment variable `AZURE_DEVOPS_ORG` (can contain a default organization name for parsing targets if only project name is given) is sourced from `.env` file. 
        *    So, if AZURE_DEVOPS_ORG=**MyMainOrg** and `--targets` (or AZURE_DEVOPS_TARGETS) =ProjectX,MyOtherOrg/ProjectY, the scanner will attempt to scan **MyMainOrg**/ProjectX and MyOtherOrg/ProjectY.
        ```bash
        # Using PAT
        python generate_codejson.py azure --az-tk YOUR_AZURE_PAT --targets MyAzureOrg/ProjectA
        # Using Service Principal
        python generate_codejson.py azure --az-cid "xxxx" --az-cs "xxxx" --az-tid "xxxx" --targets MyAzureOrg/ProjectB --limit 3
        ```
    *   *Output: Generates `intermediate_azure_<OrgName_ProjectName>.json` files.*

4.  **`merge`**: Merge all intermediate `*.json` files into the final catalog.
    *   This command looks for all `intermediate_*.json` files in your configured `OutputDir` (default: `output/`).
    *   It combines them into a single file named according to `catalogJsonFile` in your `.env` (default: `code.json`).
    *   *Note: This command also backs up existing `ExemptedCSVFile` and `PrivateIDCSVFile` before they might be updated by subsequent scan commands.*
    ```bash
    python generate_codejson.py merge
    ```

### Example Workflow (Scan all configured targets and merge)

This example demonstrates scanning GitHub organizations, GitLab groups, ADO projects and then merging the results.
Authentication details and other parameters can be provided via CLI. If not provided, the scanner will use the values from the `.env` file.

```bash
# Scan GitHub organizations listed in the .env file (GITHUB_ORGS)
python generate_codejson.py github --gh-tk <YOUR_GITHUB_PAT> 
# Scan GitLab groups listed in the .env file (GITLAB_GROUPS). The GitLab URL is set in the .env file (GITLAB_URL)
python generate_codejson.py gitlab --gl-tk <YOUR_GITLAB_PAT> 
# Scan Azure DevOps projects-pairs listed in the .env file (AZURE_DEVOPS_TARGETS).
python generate_codejson.py azure --az-tk <YOUR_ADO_PAT> 
# Merge all generated intermediate files
python generate_codejson.py merge
```

This will produce:
- Individual `intermediate_*.json` files for each scanned organization/group/project.
- Individual log files in `output/logs/` for each target scan.
- A main log file `logs/generate_codejson_main.log`.
- The final merged `code.json` (or as configured by `catalogJsonFile`).
- Updated `exempted_log.csv` and `privateid_mapping.csv` (or as configured).

### Debug Mode

You can limit the total number of repositories processed across all targets in a single run by setting the `LimitNumberOfRepos` variable in your `.env` file (set to None or leave blank for no limit).
You can limit the total number of repositories processed for a specific scan run using the `--limit <number>` option with the `github`, `gitlab`, or `azure` commands. Alternatively, for a global default limit (if no CLI `--limit` is used), you can set the `LimitNumberOfRepos` variable in your `.env` file (set to 0 or leave it blank/commented out for no limit).
```
LimitNumberOfRepos=10 
```
This is useful for quick tests and debugging.

## Output Files

- **`<OutputDir>/<catalogJsonFile>`** (e.g., `output/code.json`): The final merged catalog file.
- **`<OutputDir>/intermediate_*.json`**: Temporary JSON files, one for each scanned GitHub org, GitLab group, or Azure DevOps project. These are consumed by the `merge` command.
- **`<OutputDir>/logs/`**: Contains individual log files for each target scan (e.g., `github_MyOrg.log`) and a main script log (`generate_codejson_main.log`).
- **`<OutputDir>/<ExemptedCSVFile>`** (e.g., `output/exempted_log.csv`): CSV log of repositories identified as exempt.
- **`<OutputDir>/<PrivateIDCSVFile>`** (e.g., `output/privateid_mapping.csv`): CSV mapping of private repositories to their generated PrivateIDs and contact emails.

## Troubleshooting

- Check the log files in the `output/logs/` directory and `logs/generate_codejson_main.log` for detailed error messages.
- Ensure your API tokens have the correct permissions and are not expired.
- Verify that the organization, group, or project names/paths specified in `.env` or via CLI are correct.
- If using AI features, ensure your `GOOGLE_API_KEY` is correctly set and the API is enabled for your project.


## 🛠 Maintainers
- CDC / Enterprise DevSecOps (EDSO) - [Boris Ning](mailto:tpz7@cdc.gov?subject=Question%20on%20theShareITAct_RepoScanning&body=Let's%20discuss...)
- CDC /EA [Ricky F](https://github.com/OCIO-ricky)