# 🏛️ Share IT Act – Repository Scanning Tool

This repository contains a Python-based scanning utility designed to discover, analyze, and catalog custom-developed source code across CDC repositories. The tool supports compliance with the [SHARE IT Act (Public Law 118-187)](https://www.congress.gov/bill/118th-congress/house-bill/1390) by generating machine-readable `code.json` metadata following [code.gov schema v2.0](https://code.gov/meta/schema/2.0.0/schema.json).

## 🚀 Features

- Leverages AI to:
  - Infer code sharing exemptions based on repository metadata and content
  - Predict organization or office names for improved metadata accuracy
- Can scan the following repository platforms:
  - GitHub
  - GitLab
  - Azure DevOps
- Extracts structured metadata for public and private repositories
- Detects exemption flags and classifications
- Estimates labor hours based on repo commit history.
- Generates valid `code.json` entries
- Runs standalone or inside Docker
- Output saved for inventory consolidation and publication

## 📁 Project Structure

```
ShareITAct_RepoScanning/
├── clients/                      # Repository Platforms API connectors
    ├── github_connector.py       # GitHub API scanner
    ├── gitlab_connector.py       # GitLab API scanner
    ├── azure_devops_connector.py # Azure DevOps API scanner
├── utils/                        # Helper functions
    ├── exemption_processor.py    # AI-driven exemption detection and handler code
├── zscaler/                      # (Optional) Corporate certificates (e.g., Zscaler root CA) for trusted HTTPS access inside Docker
├── .env                          # Environment credentials
├── generate_codejson.py          # (main) Runs and builds code.json
├── requirements.txt              # Python dependencies
├── Dockerfile                    # Container build
└── output/                       # Generated reports and artifacts
```

## 🧰 Setup

Clone the repository:

```bash
git clone https://github.com/OCIO-ricky/ShareITAct_RepoScanning.git
cd ShareITAct_RepoScanning
```

Create and activate a virtual environment:

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```
**Configure Environment Variables:**
Copy the `docs/.env.template` file (or create a new file named `.env`) in the project root directory (where `generate_codejson.py` is located).
```bash
cp docs/.env.template .env 
```
Then, edit the `.env` file and populate it with your specific settings and credentials. **Do not commit the `.env` file with your actual secrets to version control.**


## Running the Tool (CLI)
The main script `generate_codejson.py` provides a Command-Line Interface (CLI) to perform scans and merge results.

All commands should be run from the root directory of the project where `generate_codejson.py` is located. Make sure your virtual environment is activated.

### General Usage

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

## 🐳 Docker Usage

To run the container in Docker. You must then type and execute the command(s) inside the container's "Exec" tab (console)
```bash
docker-compose up --build -d
```
To run the container and also execute a command to run the scanner ...
```bash
docker-compose exec app python generate_codejson.py <command> [options]
```
To stop the container:
```bash
docker-compose stop
or docker-compose down  (to also delete the container)
```
## 🔐 Configuration

Check the `.env` file in the root directory to configure the following:
- **`OutputDir`**: Directory to store output files.
- **`catalogJsonFile`**: Name of the final merged catalog file.
- **`ExemptedCSVFile`**: Name of the CSV file to log exempted repositories.
- **`PrivateIDCSVFile`**: Name of the CSV file to log private repositories and their generated PrivateIDs.
- **`GOOGLE_API_KEY`**: Google API key for AI features.

## 📤 Output

After running the **merge** command to combine all the intermidiary .json files found in the /output directtory, a new metadata catalog **code.json** is produced:

- `output/code.json`: Machine-readable metadata export that conforms to the code.gov schema
- `output/exempted_log.csv`: List of repositories inferred to be exempt, including exemption codes and justification texts (for validation and audit)
- `output/privateid_mapping.csv`: Maps anonymized private repository identifiers to known contact emails, used for metadata traceability

## ✅ Compliance Goal

Support CDC and other federal agencies in meeting SHARE IT Act requirements by generating and publishing a machine-readable `code.json`.

📤 To finalize compliance:
```bash
cp output/code.json /var/www/html/code.json
```

Published endpoint:
```
https://www.cdc.gov/code.json
```

## Override Metadata via README.md

Developers can enhance the metadata collected by this scanner by adding specific markers anywhere within their repository's existing README.md file. Adding these is completely optional, but recommended for accuracy where applicable. The scanner looks for lines starting with the following (case-insensitive). Here is an example:

***Version:*** <span style="color:darkgray">2.1.0</span>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span style="color:gray">(Specifies the current official release version of the software.)</span><br>
***Status:*** <span style="color:darkgray">Maintained</span>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span style="color:gray">(Indicates the project's lifecycle stage (e.g., Maintained, Deprecated, Experimental)).</span><br>
***Keywords:*** <span style="color:darkgray">data analysis, python, visualization</span>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span style="color:gray">(Lists relevant terms (tags) describing the project's domain or technology.</span><br>
***Labor Hours:*** <span style="color:darkgray">2500</span>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span style="color:gray">(Provides a numeric estimate of total person-hours invested across all versions.)</span><br>
***Organization:*** <span style="color:darkgray">National Center for Chronic Disease Prevention and Health Promotion (NCCDPHP)</span>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span style="color:gray">(owning CDC's Organization)</span><br>
***Contract#:*** <span style="color:darkgray">75D30123C12345</span>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span style="color:gray">(Lists the relevant government contract number, if applicable.)</span><br>
***Exemption:*** <span style="color:darkgray">exemptByLaw</span>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span style="color:gray">(Declares a specific code-sharing exemption reason (requires justification)).</span><br>
***Exemption justification:*** <span style="color:darkgray">This specific module interfaces with classified national security systems...)</span>&nbsp;&nbsp;<span style="color:gray">(Provides the mandatory explanation for the chosen Exemption.)</span><br>


## 🛠 Maintainers
- CDC / Enterprise DevSecOps (EDSO) - [Boris Ning](mailto:tpz7@cdc.gov?subject=Question%20on%20theShareITAct_RepoScanning&body=Let's%20discuss...)
- CDC /EA [Ricky F](https://github.com/OCIO-ricky)
