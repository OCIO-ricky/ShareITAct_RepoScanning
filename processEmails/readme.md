# Email Processing and Forwarding Script

This Python script automates the processing of emails from an Outlook 365 mailbox. It searches for specific `PrivateID`s in email subject lines, matches them against a `privateid_mappings.csv` file, and forwards the email requests to the appropriate code owners.

## Features

*   Connects to Outlook 365 using IMAP and SMTP via OAuth 2.0 for secure authentication.
*   Parses a CSV file (`privateid_mappings.csv`) to map `PrivateID`s to code owner contact emails.
*   Scans a specified mailbox folder (e.g., INBOX) for unread emails with subject line as stated in the environment vaariable `TARGET_SUBJECT`: 
    ```
    TARGET_SUBJECT="Request for source code repository: [privateid]"
    ```
*   Identifies `PrivateID`s in email subjects.
*   Forwards emails to contact emails associated with the found `PrivateID`.
*   Moves processed emails to a "Processed" folder and emails requiring attention to a "Manual Review" folder.
*   Configurable through an `.env` file.

## Prerequisites

1.  **Python 3.7+**
2.  **Required Python Libraries**: Install them using the `requirements.txt` file:
    ```bash
    pip install -r requirements.txt
    ```
    The `requirements.txt` file should contain:
    ```
    python-dotenv
    imap-tools
    msal
    ```
3.  **Azure AD App Registration**:
    *   An application must be registered in Azure Active Directory.
    *   The App Registration needs the following API permissions (admin consented):
        *   `https://outlook.office365.com/IMAP.AccessAsUser.All` (or equivalent Application permission like `Mail.ReadWrite` if configured for application access policies)
        *   `https://outlook.office365.com/SMTP.Send` (or equivalent Application permission like `Mail.Send`)
    *   A client secret must be generated for this app.
    *   You will need the Application (Client) ID, Directory (Tenant) ID, and the Client Secret.
4.  **`privateid_mappings.csv` File**:
    *   This CSV file maps `PrivateID`s to contact emails.
    *   It should be located in the path specified by `PRIVATEID_MAPPINGS_CSV_PATH` in the `.env` file (default is `output/privateid_mappings.csv` relative to the script's execution directory).
    *   **Format**:
        ```csv
        PrivateID,RepositoryName,RepositoryURL,Organization,ContactEmails,DateAdded
        github_12345,my-repo,https://...,CDC,user1@example.com;user2@example.com,2023-01-01T...
        another_id,other-repo,https://...,Agency,user3@example.com,2023-01-02T...
        ```
        The script primarily uses the `PrivateID` and `ContactEmails` columns. Multiple emails in `ContactEmails` should be semicolon-separated.

## Configuration

The script is configured using an `.env` file in the same directory as the script. Create a `.env` file by copying the `.env.template` and filling in your specific values.

**`.env` file variables:**

*   `OUTLOOK_IMAP_SERVER`: IMAP server address (e.g., `outlook.office365.com`).
*   `OUTLOOK_IMAP_PORT`: IMAP server port (e.g., `993`).
*   `OUTLOOK_EMAIL_ADDRESS`: The email address of the mailbox the script will access (service account email).
*   `OUTLOOK_SMTP_SERVER`: SMTP server address (e.g., `smtp.office365.com`).
*   `OUTLOOK_SMTP_PORT`: SMTP server port (e.g., `587`).
*   `AZURE_CLIENT_ID`: Your Azure AD App Registration's Application (Client) ID.
*   `AZURE_TENANT_ID`: Your Azure AD Directory (Tenant) ID.
*   `AZURE_CLIENT_SECRET`: The client secret generated for your Azure AD App.
*   `OAUTH_SCOPE`: OAuth scope (default: `https://outlook.office365.com/.default`).
*   `PRIVATEID_MAPPINGS_CSV_PATH`: Path to the `privateid_mappings.csv` file.
*   `IMAP_MAILBOX_TO_CHECK`: The mailbox folder to scan for new emails (e.g., `INBOX`).
*   `IMAP_PROCESSED_FOLDER`: Folder to move successfully processed emails to (e.g., `ProcessedRequests`). The script will attempt to create it if it doesn't exist.
*   `IMAP_MANUAL_REVIEW_FOLDER`: Folder to move emails that couldn't be automatically processed or require manual attention (e.g., `NeedsManualReview`). The script will attempt to create it if it doesn't exist.
*   `TARGET_SUBJECT` (Optional): A specific subject line pattern to target. Use `[privateid]` as a placeholder for the ID (e.g., `"Request for source code repository: [privateid]"`). If set, only emails matching this subject pattern (and are unread) will be processed for ID extraction. If left blank, the script searches for any known `privateid` in the subject of unread emails.

**Example `.env.template`:**
```env
# Outlook IMAP Settings
OUTLOOK_IMAP_SERVER="outlook.office365.com"
OUTLOOK_IMAP_PORT="993"
OUTLOOK_EMAIL_ADDRESS="your_service_account_email@yourdomain.com" # The mailbox to access

# Outlook SMTP Settings (for forwarding)
OUTLOOK_SMTP_SERVER="smtp.office365.com"
OUTLOOK_SMTP_PORT="587"
# SMTP username is often the same as OUTLOOK_EMAIL_ADDRESS

# Azure AD App Registration Details (for OAuth 2.0)
AZURE_CLIENT_ID="your_app_client_id"
AZURE_TENANT_ID="your_directory_tenant_id"
AZURE_CLIENT_SECRET="your_app_client_secret"

# OAuth Scopes
OAUTH_SCOPE="https://outlook.office365.com/.default"

# CSV File Path
PRIVATEID_MAPPINGS_CSV_PATH="output/privateid_mappings.csv"

# Mailbox Folders
IMAP_MAILBOX_TO_CHECK="INBOX"
IMAP_PROCESSED_FOLDER="ProcessedRequests"
IMAP_MANUAL_REVIEW_FOLDER="NeedsManualReview"

TARGET_SUBJECT=
```

## Setup

1.  **Clone the repository** (if applicable) or place the script (`process_email_requests.py`) in your desired directory.
2.  **Create `requirements.txt`** in the script's directory with the content mentioned in the Prerequisites section.
3.  **Install dependencies**:
    ```bash
    pip install -r requirements.txt
    ```
4.  **Create and configure your `.env` file** based on `.env.template`.
5.  **Ensure your `privateid_mappings.csv` file** is present at the location specified in `PRIVATEID_MAPPINGS_CSV_PATH` and is correctly formatted.
6.  **Verify Azure AD App Permissions**: Ensure the service principal associated with your Azure AD App has been granted the necessary permissions to access the target mailbox. This might involve Exchange Online PowerShell cmdlets like `Add-MailboxPermission` or configuring application access policies, depending on your permission model (delegated vs. application).

## Running the Script

Navigate to the directory containing the script and run:

```bash
python process_email_requests.py
```

The script will log its actions to the console.

## Important Notes

*   **Security**:
    *   Protect your `.env` file, especially the `AZURE_CLIENT_SECRET`. Ensure it's not committed to version control (it should be in your `.gitignore`).
    *   Follow the principle of least privilege when granting API permissions in Azure AD.
*   **Testing**:
    *   Thoroughly test the script with a non-production mailbox first.
    *   Initially, you might want to set `fetch_criteria = 'ALL'` (instead of `AND(seen=False)`) in the `process_mailbox` function for testing with existing emails in a test folder, but remember to change it back for production to only process new/unread emails.
    *   Ensure the `IMAP_PROCESSED_FOLDER` and `IMAP_MANUAL_REVIEW_FOLDER` exist in the mailbox, or that the service account has permission to create them.
*   **Error Handling**: The script includes basic error handling and logging. Monitor the logs for any issues.
*   **Idempotency**: The script marks emails as seen and attempts to move them after processing to prevent reprocessing. If an email is processed but the move fails, it might be picked up again if it remains unread. The folder-moving strategy is key to robust processing.

```
