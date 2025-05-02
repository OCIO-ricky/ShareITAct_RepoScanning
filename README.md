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



## 🐳 Docker Usage

To build and run inside Docker:

```bash
docker build -t shareitact_scan .
docker run --env-file .env -v $(pwd)/output:/app/output shareitact_scan
```

## 🔄 Run the Generator

To run locally:

```bash
python generate_codejson.py
```

## 🔐 Configuration

Create a `.env` file in the root directory to securely store all required tokens and credentials:

```env
GITHUB_TOKEN=your_token
GITLAB_TOKEN=your_token
AZURE_DEVOPS_TOKEN=your_token

AI_MODEL_PROVIDER=openai
AI_API_KEY=your_openai_or_other_api_token
```

## 🧪 Test Individual Connectors

You can run each connector script directly to test its connection and basic data fetching capabilities for a specific platform.

*(Note: Running connectors directly is primarily for testing the connection and data retrieval logic. It will **not** perform the full processing pipeline or generate the final `code.json`, `exempted_log.csv`, or `privateid_mapping.csv` files. The output will typically be printed to the console.)*

```bash
python clients/github_connector.py
python clients/gitlab_connector.py
python clients/azure_devops_connector.py
```

## 📤 Output

Successful runs produce:

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

## 🛠 Maintainers
- CDC / Enteprise Data Services Office (EDSO) 
- CDC / [Ricky F](https://github.com/OCIO-ricky)
