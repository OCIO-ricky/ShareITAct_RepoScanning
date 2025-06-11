# Email Processing and Forwarding Script

This Python script automates the processing of emails from an Outlook 365 mailbox. It searches for specific `PrivateID`s in email subject lines, matches them against a `privateid_mappings.csv` file, and forwards the email requests to the appropriate code owners.
**Note:** This version uses Basic Authentication (username/password). The authenticating service account may need delegate/full access permissions if scanning a shared mailbox different from its own.

## Features

*   Connects to Outlook 365 using IMAP (for reading) and SMTP (for sending) via Basic Authentication.
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
1.  **Python 3.x**: Ensure Python is installed on the system where the script will run.
2.  **Azure App Registration**:
   *   An Azure Active Directory (Azure AD) App Registration is required for the script to authenticate with Microsoft Graph API.
   *   Your IT support/Azure administrator needs to create this App Registration.
   *   The App Registration must be granted the following **Application Permissions** for Microsoft Graph API:
       *   `Mail.ReadWrite`: To read, move, and mark emails as read in the target mailbox.
       *   `Mail.Send`: To send emails (for forwarding).
   *   **Admin consent** will likely be required for these permissions.
   *   It is highly recommended to scope the `Mail.ReadWrite` permission to *only* the specific mailbox the application needs to access (e.g., `shareit@cdc.gov`) using an Application Access Policy in Exchange Online.
3.  **Required Information from IT HelpDesk**:
    *   `Client ID` (Application ID) of the Azure App Registration.
    *   `Client Secret` (Application Password) generated for the App Registration.
    *   `Tenant ID` (Directory ID) of your Azure AD instance.
    *   **Example IT Helpdesk Request:**
        ```
        Subject: Azure App Registration for Email Processing Script (shareit@cdc.gov)

        Hi Team,

        Could you please create an Azure App Registration for a Python script that needs to process emails in the 'shareit@cdc.gov' mailbox?
        The script will use the Microsoft Graph API with Client Credentials Flow. We will require the following Application Permissions: Mail.ReadWrite and Mail.Send. Please also scope Mail.ReadWrite to only the 'shareit@cdc.gov' mailbox if possible.

        Once created, please provide the Client ID, Client Secret, and Tenant ID for this App Registration.
        ```

## Setup
1.  **Python 3.7+**
2.  **Required Python Libraries**: Install them using the `requirements.txt` file:
    ```bash
    pip install -r requirements.txt
    ```
    The `requirements.txt` file should contain:
    ```
    python-dotenv
    imap-tools
    ```
3.  **`privateid_mappings.csv` File**:
    *   This CSV file maps `PrivateID`s to contact emails.
    *   It should be located in the path specified by `PRIVATEID_MAPPINGS_CSV_PATH` in the `.env` file (default is `output/privateid_mappings.csv` relative to the script's execution directory).
    *   **Format**:
        ```csv
        PrivateID,RepositoryName,RepositoryURL,Organization,ContactEmails,DateAdded
        ```
        The script primarily uses the `PrivateID` and `ContactEmails` columns. Multiple emails in `ContactEmails` should be semicolon-separated.

## Configuration

   Create a `.env` file in the script's directory by copying `.env.template`.<br>
   Update the `.env` file with the necessary credentials and configuration:
```dotenv
# --- Microsoft Graph API Authentication Settings ---
GRAPH_CLIENT_ID=    "YOUR_AZURE_APP_CLIENT_ID"
GRAPH_CLIENT_SECRET="YOUR_AZURE_APP_CLIENT_SECRET_VALUE"
GRAPH_TENANT_ID=    "YOUR_AZURE_TENANT_ID"

# --- Mailbox Configuration ---
TARGET_MAILBOX_EMAIL_TO_SCAN="shareit@cdc.gov" 

# --- File and Folder Settings ---
PRIVATEID_MAPPINGS_CSV_PATH="output/privateid_mappings.csv"
MAILBOX_FOLDER_TO_CHECK="Inbox"
PROCESSED_FOLDER_NAME="ProcessedRequests" # Optional
MANUAL_REVIEW_FOLDER_NAME="NeedsManualReview" # Optional
TARGET_SUBJECT="Request for source code repository: [privateid]" # Optional
```
**Important**:
*   Replace placeholder values (like `YOUR_AZURE_APP_CLIENT_ID`) with the actual credentials provided by your IT support.
*   `TARGET_MAILBOX_EMAIL_TO_SCAN` is the email address of the mailbox the script will monitor (e.g., the shared mailbox `shareit@cdc.gov`).


## Running the Script

Navigate to the directory containing the script and run:

```bash
python process_email_requests.py
```

The script will log its actions to the console.

## Important Notes

*   **Security**:
    *   Protect your `.env` file, especially the `OUTLOOK_SERVICE_ACCOUNT_PASSWORD`. Ensure it's not committed to version control (it should be in your `.gitignore`).
    *   Basic Authentication is inherently less secure than OAuth 2.0. Use with caution.
*   **Testing**:
    *   Thoroughly test the script with a non-production mailbox first.
    *   Initially, you might want to set `fetch_criteria = 'ALL'` (instead of `AND(seen=False)`) in the `process_mailbox` function for testing with existing emails in a test folder, but remember to change it back for production to only process new/unread emails.
    *   Ensure the `IMAP_PROCESSED_FOLDER` and `IMAP_MANUAL_REVIEW_FOLDER` exist in the mailbox, or that the service account has permission to create them.
*   **Error Handling**: The script includes basic error handling and logging. Monitor the logs for any issues.
*   **Idempotency**: The script marks emails as seen and attempts to move them after processing to prevent reprocessing. If an email is processed but the move fails, it might be picked up again if it remains unread. The folder-moving strategy is key to robust processing.
