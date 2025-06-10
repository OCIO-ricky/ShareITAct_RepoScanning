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
        github_12345,my-repo,https://...,CDC,user1@example.com;user2@example.com,2023-01-01T...
        another_id,other-repo,https://...,Agency,user3@example.com,2023-01-02T...
        ```
        The script primarily uses the `PrivateID` and `ContactEmails` columns. Multiple emails in `ContactEmails` should be semicolon-separated.

## Configuration

The script is configured using an `.env` file in the same directory as the script. Create a `.env` file by copying the `.env.template` and filling in your specific values.

**`.env` file variables:**

*   `OUTLOOK_IMAP_SERVER`: IMAP server address (e.g., `outlook.office365.com`).
*   `OUTLOOK_IMAP_PORT`: IMAP server port (e.g., `993`).
*   `OUTLOOK_SERVICE_ACCOUNT_EMAIL_ADDRESS`: The email address of the service account used for **authentication**.
*   `OUTLOOK_SERVICE_ACCOUNT_PASSWORD`: The password for the authenticating service account.
*   `TARGET_MAILBOX_EMAIL_TO_SCAN`: The email address of the mailbox the script will **scan** (e.g., `shareit@cdc.gov`). 
*   `OUTLOOK_SMTP_SERVER`: SMTP server address (e.g., `smtp.office365.com`).
*   `OUTLOOK_SMTP_PORT`: SMTP server port (e.g., `587`).
*   `PRIVATEID_MAPPINGS_CSV_PATH`: Path to the `privateid_mappings.csv` file.
 attempt to create it if it doesn't exist.
*   `TARGET_SUBJECT` (Optional): A specific subject line pattern to target. Use `[privateid]` as a placeholder for the ID (e.g., `"Request for source code repository: [privateid]"`). If set, only emails matching this subject pattern (and are unread) will be processed for ID extraction. If left blank, the script searches for any known `privateid` in the subject of unread emails.

**Example `.env.template`:**
```env
# Outlook IMAP Settings
OUTLOOK_IMAP_SERVER="outlook.office365.com"
OUTLOOK_IMAP_PORT="993"

# Credentials for the account that will perform the login
OUTLOOK_SERVICE_ACCOUNT_EMAIL_ADDRESS="your_authenticating_service_account_email@yourdomain.com"
OUTLOOK_SERVICE_ACCOUNT_PASSWORD="password_for_authenticating_service_account"

# Target mailbox to scan (e.g., a shared mailbox like shareit@cdc.gov).
# Leave blank to scan the service account's own mailbox.
# The service account must have delegate/full access permissions to this mailbox if it's different.
TARGET_MAILBOX_EMAIL_TO_SCAN="email_address_of_mailbox_to_scan@yourdomain.com"

# Outlook SMTP Settings (for forwarding)
OUTLOOK_SMTP_SERVER="smtp.office365.com"
OUTLOOK_SMTP_PORT="587"

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
6.  **Verify Service Account Access**:
    *   Ensure the service account (`OUTLOOK_SERVICE_ACCOUNT_EMAIL_ADDRESS`) has IMAP and SMTP access enabled, and that Basic Authentication is permitted.
    *   If `TARGET_MAILBOX_EMAIL_TO_SCAN` is different from the service account, ensure the service account has "Full Access" (delegate) permissions to the target mailbox. This is typically configured by an Exchange/Microsoft 365 administrator.

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

```
