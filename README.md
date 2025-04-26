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
