# processEmails/processEmails.py
#
# Make sure your privateid_mappings.csv file is in the /output/ folder
# (or update the PRIVATEID_MAPPINGS_CSV_PATH in your .env file to its correct location).
#
# $> cd processMails
# $> python process_email_requests.py
#
# Test thoroughly in a development environment.
from datetime import datetime, timedelta, timezone
import os
import csv
import re
import base64
import logging
import requests
import json # For parsing Graph API error responses
import sys
# import base64 # No longer needed here if library handles encoding from bytes
from dotenv import load_dotenv
from O365 import Account, MSGraphProtocol
from O365.message import Message as O365Message # For type hinting
from O365.mailbox import MailBox as O365Mailbox # For type hinting

# --- Configuration ---
# Logging Setup
LOG_DIR = "logs"
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

# Use a fixed log file name to append to the same file on each run
log_file_name = os.path.join(LOG_DIR, "process_emails.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_name),
        logging.StreamHandler(sys.stdout) # Keep logging to console as well
    ]
)
logger = logging.getLogger(__name__)

# --- Visual Separator for Log File ---
def log_run_separator():
    """Logs a visual separator for a new script run."""
    separator = "=" * 80
    start_time_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S %Z")
    logger.info(separator)
    logger.info(f"SCRIPT EXECUTION STARTED: {start_time_str}")
    logger.info(separator)

# Load environment variables from .env file
# Ensure python-dotenv is installed: pip install python-dotenv O365
load_dotenv()

DOTENV_PATH = load_dotenv(override=True) # Find and load .env, override allows re-load if needed
# --- Environment Variable Retrieval and Validation ---
def get_env_var(var_name, is_required=True, default=None):
    """Retrieves an environment variable, with optional requirement and default."""
    value = os.getenv(var_name)
    if is_required and not value:
        logger.error(f"Missing required environment variable: {var_name}")
        raise ValueError(f"Missing required environment variable: {var_name}")
    return value if value else default

try:
    # Mailbox to Scan (Required)
    # This is the email address of the mailbox (e.g., shared mailbox) the application will process.
    TARGET_MAILBOX_EMAIL_TO_SCAN = get_env_var("TARGET_MAILBOX_EMAIL_TO_SCAN", is_required=True).strip()

    # Graph API Settings (for OAuth Client Credentials Flow)
    GRAPH_CLIENT_ID = get_env_var("GRAPH_CLIENT_ID")
    GRAPH_CLIENT_SECRET = get_env_var("GRAPH_CLIENT_SECRET")
    GRAPH_TENANT_ID = get_env_var("GRAPH_TENANT_ID")

    # File and Folder Settings
    PRIVATEID_MAPPINGS_CSV_PATH = get_env_var("PRIVATEID_MAPPINGS_CSV_PATH", default="output/privateid_mappings.csv")
    MAILBOX_FOLDER_TO_CHECK = get_env_var("MAILBOX_FOLDER_TO_CHECK", default="Inbox") # Folder name within the target mailbox
    PROCESSED_FOLDER_NAME = get_env_var("PROCESSED_FOLDER_NAME", is_required=False) # Folder name for processed emails
    MANUAL_REVIEW_FOLDER_NAME = get_env_var("MANUAL_REVIEW_FOLDER_NAME", is_required=False) # Folder name for manual review
    TARGET_SUBJECT = get_env_var("TARGET_SUBJECT", is_required=False, default="").strip()

    # Configuration for Manual Client Secret Renewal Reminder
    CURRENT_SECRET_EXPIRY_DATE_STR = get_env_var("CURRENT_SECRET_EXPIRY_DATE", is_required=False) # Format: YYYY-MM-DD. Used for reminder.

except ValueError as e:
    logger.critical(f"Configuration error: {e}. Please check your .env file. Exiting.")
    exit(1)

# --- Helper Functions ---

def load_privateid_mappings(csv_path: str) -> dict:
    """Loads privateid to email mappings from a CSV file."""
    mappings = {}
    try:
        with open(csv_path, mode='r', encoding='utf-8') as infile:
            reader = csv.DictReader(infile)
            if not reader.fieldnames:
                logger.error(f"CSV file {csv_path} is empty or has no header row.")
                return mappings

            normalized_fieldnames = {name.lower(): name for name in reader.fieldnames}

            private_id_col_name = normalized_fieldnames.get('privateid')
            contact_emails_col_name = normalized_fieldnames.get('contactemails')

            if not private_id_col_name:
                logger.error(f"'PrivateID' column not found in {csv_path}. Please ensure it exists.")
                return mappings
            if not contact_emails_col_name:
                logger.warning(f"'ContactEmails' column not found in {csv_path}. Will not be able to forward emails.")
                # Continue, but emails won't be found for forwarding

            for row_num, row_dict in enumerate(reader, 1):
                private_id = row_dict.get(private_id_col_name, "").strip()
                if not private_id:
                    logger.warning(f"Skipping empty or invalid PrivateID in row {row_num+1} in {csv_path}")
                    continue

                emails_str = ""
                if contact_emails_col_name:
                    emails_str = row_dict.get(contact_emails_col_name, "").strip()

                emails = [email.strip() for email in emails_str.split(';') if email.strip()]


                if private_id in mappings:
                    logger.warning(f"Duplicate PrivateID '{private_id}' found at row {row_num+1} in {csv_path}. Using the first occurrence's emails.")
                else:
                    mappings[private_id] = emails
        logger.info(f"Successfully loaded {len(mappings)} mappings from {csv_path}")
    except FileNotFoundError:
        logger.error(f"PrivateID mappings CSV file not found: {csv_path}")
        raise
    except Exception as e:
        logger.error(f"Error loading PrivateID mappings CSV: {e}")
        raise
    return mappings



def forward_email_message_graph(
    target_o365_mailbox,  # O365Mailbox
    original_o365_msg,    # O365Message
    to_emails: list,
    subject_prefix="Fwd: ",
    account=None          # <-- NEW: optional O365 Account object
):
    """
    Forwards the original O365 Message object to specified recipients by creating a new email
    and attaching the original as a .eml file. If the O365 library fails, falls back to a manual
    Microsoft Graph API call.
    """
    if not to_emails or not all(isinstance(addr, str) for addr in to_emails):
        logger.warning("No valid recipient emails provided for forwarding.")
        return False

    # Compose subject and body
    original_subject = original_o365_msg.subject if original_o365_msg.subject else "No Subject"
    subject = f"{subject_prefix}{original_subject}"
    intro_text = (
        f"Hello,\n\nThe following email request regarding '{original_subject}' "
        f"(received from: {original_o365_msg.sender.address if original_o365_msg.sender else 'Unknown Sender'}) "
        "is being forwarded for your attention.\n\n"
        "Regards,\nShareIT Auto-Processor\n\n"
        "--- Original Message Below ---"
    )

    # Manual Graph API call
    try:
        # Use the global credentials directly instead of trying to extract from O365 connection
        # This is more reliable for client credentials flow
        
        # Get a fresh token using MSAL or direct OAuth2 call
        token_url = f"https://login.microsoftonline.com/{GRAPH_TENANT_ID}/oauth2/v2.0/token"
        token_data = {
            'client_id': GRAPH_CLIENT_ID,
            'client_secret': GRAPH_CLIENT_SECRET,
            'scope': 'https://graph.microsoft.com/.default',
            'grant_type': 'client_credentials'
        }
        
        logger.debug("Requesting fresh access token...")
        token_response = requests.post(token_url, data=token_data, timeout=30)
        
        if token_response.status_code != 200:
            logger.error(f"Failed to get access token: {token_response.status_code} {token_response.text}")
            return False
            
        token_json = token_response.json()
        access_token = token_json.get('access_token')
        
        if not access_token:
            logger.error("No access token in response")
            return False

        # Build recipients - ensure proper format
        to_recipients = []
        for email in to_emails:
            if email and isinstance(email, str):
                to_recipients.append({
                    "emailAddress": {
                        "address": email.strip()
                    }
                })

        if not to_recipients:
            logger.error("No valid recipients for manual Graph API call.")
            return False

        # Get mailbox address for Graph API
        mailbox_address = getattr(target_o365_mailbox, 'address', None) or TARGET_MAILBOX_EMAIL_TO_SCAN
        if not mailbox_address:
            logger.error("No mailbox address found for manual Graph API call.")
            return False

        # Build attachment (if possible)
        attachments = []
        try:
            original_mime_content = original_o365_msg.get_mime_content()
            if original_mime_content:
                # Ensure the MIME content is properly encoded
                if isinstance(original_mime_content, bytes):
                    content_bytes = base64.b64encode(original_mime_content).decode('ascii')
                else:
                    content_bytes = base64.b64encode(original_mime_content.encode('utf-8')).decode('ascii')
                
                attachments.append({
                    "@odata.type": "#microsoft.graph.fileAttachment",
                    "name": "forwarded_request.eml",
                    "contentType": "message/rfc822",
                    "contentBytes": content_bytes
                })
                logger.debug("Successfully prepared .eml attachment")
            else:
                logger.warning("Could not retrieve MIME content for attachment")
        except Exception as attach_e:
            logger.warning(f"Failed to prepare attachment: {attach_e}. Sending without attachment.")
            attachments = []

        # Compose the message payload - ensure all required fields are present
        message_payload = {
            "message": {
                "subject": subject,
                "body": {
                    "contentType": "Text",
                    "content": intro_text
                },
                "toRecipients": to_recipients,
                "importance": "normal"
            },
            "saveToSentItems": True
        }
        
        # Add attachments if any
        if attachments:
            message_payload["message"]["attachments"] = attachments

        # Temporary debug - remove after testing
      ##  logger.info(f"Full message payload: {json.dumps(message_payload, indent=2)}")

        url = f"https://graph.microsoft.com/v1.0/users/{mailbox_address}/sendMail"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        
        logger.debug(f"Sending Graph API request to: {url}")
        logger.debug(f"Recipients: {[r['emailAddress']['address'] for r in to_recipients]}")
        
        response = requests.post(url, headers=headers, json=message_payload, timeout=60)
        
        if response.status_code in (200, 202):
            logger.info(
                f"Successfully sent forwarded email (Original Subject: {original_subject}) to: {', '.join(to_emails)} via manual Graph API."
            )
            return True
        else:
            logger.error(
                f"Manual Graph API error while sending forwarded email: {response.status_code}"
            )
            try:
                error_details = response.json()
                logger.error(f"Error details: {json.dumps(error_details, indent=2)}")
            except:
                logger.error(f"Error response text: {response.text}")
            return False

    except requests.exceptions.RequestException as req_e:
        logger.error(f"Request exception during manual Graph API call: {req_e}")
        return False
    except Exception as e:
        logger.error(f"Manual Graph API exception while sending forwarded email: {e}")
        return False

def ensure_mailbox_folder_exists_graph(o365_mailbox: O365Mailbox, folder_name: str):
    """Checks if a mailbox folder exists, and creates it if not. Returns the folder object."""
    if not folder_name:
        return None
    
    target_folder = o365_mailbox.get_folder(folder_name=folder_name)
    if target_folder:
        logger.debug(f"Folder '{folder_name}' already exists in mailbox '{o365_mailbox.main_resource}'.")
        return target_folder
    
    logger.info(f"Folder '{folder_name}' does not exist in mailbox '{o365_mailbox.main_resource}'. Attempting to create it.")
    try:
        # Creates a new folder under the root of the mailbox.
        # If you need subfolders like "Parent/Child", you'd get "Parent" then create "Child" in it.
        created_folder = o365_mailbox.create_child_folder(folder_name)
        logger.info(f"Successfully created folder '{folder_name}' in mailbox '{o365_mailbox.main_resource}'.")
        return created_folder
    except Exception as e:
        logger.error(f"Could not create or verify folder '{folder_name}' in mailbox '{o365_mailbox.main_resource}': {e}")
        raise # Re-raise if folder creation is critical

def authenticate_graph_api():
    """Authenticates with Microsoft Graph API and handles secret renewal if configured."""
    # --- Initial Authentication Attempt ---
    try:
        account = None # Initialize account
        logger.info(f"Attempting to authenticate with Microsoft Graph API using Client ID: {GRAPH_CLIENT_ID} and Tenant ID: {GRAPH_TENANT_ID}")
        credentials = (GRAPH_CLIENT_ID, GRAPH_CLIENT_SECRET)
        protocol = MSGraphProtocol(api_version='v1.0', tenant_id=GRAPH_TENANT_ID) # Added tenant_id here
        account_resource = TARGET_MAILBOX_EMAIL_TO_SCAN # UPN for primary interaction
        account = Account(credentials, auth_flow_type='credentials', tenant_id=GRAPH_TENANT_ID, protocol=protocol, main_resource=account_resource)

        if not account.authenticate(scope=['https://graph.microsoft.com/.default']):
            logger.critical("Microsoft Graph API authentication failed (token acquisition problem).")
            return None

    except SystemExit: # Catch sys.exit if called by auth function after renewal
        return None # Indicate failure to proceed with email processing, let main loop handle exit

    except Exception as auth_exception:
        logger.error(f"Microsoft Graph API authentication failed: {auth_exception}")
        
        # Check for specific expired secret error (AADSTS7000222)
        # Error messages/structures can vary based on the library (MSAL underlying O365)
        auth_error_str = str(auth_exception).lower()
        if "aadsts7000215" in auth_error_str: # Invalid client secret (wrong value)
             logger.critical(f"Microsoft Graph API authentication failed: Invalid client secret provided. "
                             f"Ensure GRAPH_CLIENT_SECRET in .env is the secret *value*, not its ID. Original error: {auth_exception}")
        elif "aadsts7000222" in auth_error_str or "client secret is expired" in auth_error_str:
            logger.critical(f"Microsoft Graph API authentication failed: Client Secret has EXPIRED. Original error: {auth_exception}")
        else: # Other authentication errors
            logger.critical(f"Microsoft Graph API authentication failed. Check credentials, permissions, and tenant ID. Original error: {auth_exception}")
        return None # Signal failure
    except Exception as e:
        logger.critical(f"An unexpected error occurred during Microsoft Graph API authentication setup: {e}. Exiting.", exc_info=True)
        return None # Signal failure
    
    logger.info("Successfully authenticated with Microsoft Graph API.")
    return account

# --- Main Processing Logic ---
def process_mailbox():
    log_run_separator() # Add visual separator at the start of processing
    logger.info("Starting email processing...")

    account = authenticate_graph_api()
    if not account:
        # Authentication function already logged critical errors and exited if needed via sys.exit,
        # or returned None indicating failure to proceed.
        return False # Indicate failure to proceed with email processing

    # --- Initial Login Check ---
    # The account object is now already authenticated if we reached here.

    try:
        # --- Load Mappings (only if login was successful) ---
        privateid_mappings = load_privateid_mappings(PRIVATEID_MAPPINGS_CSV_PATH)
    except Exception:
        logger.critical("Failed to load private ID mappings. Cannot proceed.")
        return False # Signal failure

    # --- Manual Renewal Reminder Check ---
    # This check runs regardless of AUTO_RENEW_SECRET_ENABLED, as it's a manual instruction.
    # It uses the CURRENT_SECRET_EXPIRY_DATE_STR loaded from .env at the start of this run.
    if CURRENT_SECRET_EXPIRY_DATE_STR:
        try:
            # Parse the expiry date from .env
            expiry_date_obj = datetime.strptime(CURRENT_SECRET_EXPIRY_DATE_STR, "%Y-%m-%d").date()
            # Get current UTC date
            current_date_utc = datetime.now(timezone.utc).date()
            days_to_expiry = (expiry_date_obj - current_date_utc).days

            if 0 < days_to_expiry <= 10: # Within 1-10 days of expiring
                logger.warning(
                    f"ATTENTION: Client Secret is nearing expiration! Expires on {CURRENT_SECRET_EXPIRY_DATE_STR} (in {days_to_expiry} day{'s' if days_to_expiry != 1 else ''}). "
                    f"Please initiate the manual renewal process for the Azure AD App Registration and update "
                    f"GRAPH_CLIENT_SECRET and CURRENT_SECRET_EXPIRY_DATE in the .env file."
                )
            elif days_to_expiry <= 0: # Expired or expires today
                # This message will appear if the secret has expired and auto-renewal (if enabled) failed,
                # or if auto-renewal is disabled.
                logger.critical(
                    f"CRITICAL: Client Secret has EXPIRED or expires today (Expiry Date: {CURRENT_SECRET_EXPIRY_DATE_STR})! "
                    f"Manual renewal is URGENTLY required. The script may fail to authenticate or perform operations. "
                    f"Update GRAPH_CLIENT_SECRET and CURRENT_SECRET_EXPIRY_DATE in the .env file immediately."
                )
        except ValueError:
            # The proactive renewal check (if enabled) already logs a more specific warning if CURRENT_SECRET_EXPIRY_DATE_STR is unparseable.
            pass # No need for a duplicate warning here if the format is wrong.
        except Exception as e_manual_reminder:
            logger.error(f"An unexpected error occurred during the manual secret renewal reminder check: {e_manual_reminder}")

    if not privateid_mappings:
        logger.warning("No private ID mappings loaded. No emails will be processed for forwarding.")
        # return # Uncomment this if you want to exit if no mappings are found

    processed_count = 0
    forwarded_count = 0
    manual_review_count = 0

    # --- Compile Regexes ---
    target_subject_regex = None
    if TARGET_SUBJECT:
        try:
            # Escape the literal parts of TARGET_SUBJECT, then replace escaped [privateid] with regex group
            pattern_str = re.escape(TARGET_SUBJECT).replace(re.escape(r'[privateid]'), r'(\S+?)')
            target_subject_regex = re.compile(f"^{pattern_str}$", re.IGNORECASE)
            logger.info(f"Using target subject pattern: {target_subject_regex.pattern}")
        except re.error as e:
            logger.error(f"Invalid regex pattern derived from TARGET_SUBJECT '{TARGET_SUBJECT}': {e}. Will not use target subject matching.")
            target_subject_regex = None # Ensure it's None if compilation fails

    try:
        # TARGET_MAILBOX_EMAIL_TO_SCAN is now guaranteed by the initial check.
        effective_mailbox_target = TARGET_MAILBOX_EMAIL_TO_SCAN
        logger.info(f"Targeting mailbox: {effective_mailbox_target}")

        target_o365_mailbox = account.mailbox(resource=effective_mailbox_target)
        if not target_o365_mailbox:
            logger.error(f"Could not access mailbox for {effective_mailbox_target}. Check permissions. Exiting.")
            return False # Signal failure
        logger.info(f"Successfully connected to mailbox: {effective_mailbox_target}")

        folder_to_scan = target_o365_mailbox.get_folder(folder_name=MAILBOX_FOLDER_TO_CHECK)
        if not folder_to_scan:
            logger.error(f"Folder '{MAILBOX_FOLDER_TO_CHECK}' not found in mailbox '{effective_mailbox_target}'. Exiting.")
            return False # Signal failure
        logger.info(f"Scanning folder: '{folder_to_scan.name}' (ID: {folder_to_scan.folder_id})")

        processed_o365_folder = None
        if PROCESSED_FOLDER_NAME:
            processed_o365_folder = ensure_mailbox_folder_exists_graph(target_o365_mailbox, PROCESSED_FOLDER_NAME)
            if not processed_o365_folder: logger.warning(f"Could not ensure 'Processed' folder '{PROCESSED_FOLDER_NAME}' exists. Emails will not be moved there.")

        manual_review_o365_folder = None
        if MANUAL_REVIEW_FOLDER_NAME:
            manual_review_o365_folder = ensure_mailbox_folder_exists_graph(target_o365_mailbox, MANUAL_REVIEW_FOLDER_NAME)
            if not manual_review_o365_folder: logger.warning(f"Could not ensure 'Manual Review' folder '{MANUAL_REVIEW_FOLDER_NAME}' exists. Emails will not be moved there.")

        # Fetch unread emails
        # O365 query: field_name__operator=value (e.g., is_read=False)
        unread_query = folder_to_scan.new_query().on_attribute('isRead').equals(False)
        logger.info(f"Fetching unread emails from '{folder_to_scan.name}'...")

        emails_to_process = list(folder_to_scan.get_messages(limit=None, query=unread_query)) # Get all matching
        logger.info(f"Found {len(emails_to_process)} unread email(s) in '{folder_to_scan.name}'.")

        if not emails_to_process:
            logger.info("No new emails to process.")
            return True # Successful run, just no emails to process

        for msg_obj in emails_to_process: # msg_obj is an O365.Message object
            msg_subject = msg_obj.subject or ""
            msg_sender = msg_obj.sender.address if msg_obj.sender else "Unknown Sender"
            msg_date = msg_obj.received.strftime('%Y-%m-%d %H:%M:%S %Z') if msg_obj.received else "Unknown Date"
            msg_id = msg_obj.object_id # Graph Message ID

            logger.info(f"Processing email ID {msg_id} - Subject: '{msg_subject}' From: '{msg_sender}' Date: '{msg_date}'")
            processed_count += 1
            # By default, assume no action will be successfully completed that warrants changing the email's state.
            # If forwarding fails, we want to skip marking as read or moving.

            matched_privateid = None
            can_attempt_forward = False
            contact_emails_for_forwarding = []

            if target_subject_regex:
                match = target_subject_regex.search(msg_subject)
                if match:
                    potential_pid = match.group(1) # The captured group for [privateid]
                    if potential_pid in privateid_mappings:
                        matched_privateid = potential_pid
                        logger.info(f"Extracted privateid '{matched_privateid}' from subject using TARGET_SUBJECT pattern.")
                        contact_emails_for_forwarding = privateid_mappings.get(matched_privateid, [])
                        if contact_emails_for_forwarding:
                            can_attempt_forward = True
                        else:
                            logger.warning(f"PrivateID '{matched_privateid}' found, but no contact emails configured. Email ID: {msg_id}")
                    else:
                        logger.warning(f"Subject matched TARGET_SUBJECT pattern, but extracted ID '{potential_pid}' not in mappings. Email ID: {msg_id}, Subject: '{msg_subject}'")
                else:
                    logger.info(f"Email subject '{msg_subject}' did not match TARGET_SUBJECT pattern. Email ID: {msg_id}")
            # else: No target_subject_regex, so can_attempt_forward remains False.
            
            if can_attempt_forward:
                logger.info(f"Attempting to forward email for privateid '{matched_privateid}' to: {contact_emails_for_forwarding}")
                if forward_email_message_graph(target_o365_mailbox, msg_obj, contact_emails_for_forwarding, account=account):
                    # Forwarding SUCCEEDED
                    forwarded_count += 1
                    msg_obj.mark_as_read()
                    if processed_o365_folder:
                        logger.info(f"Moving successfully forwarded email to '{processed_o365_folder.name}'.")
#                        logger.info(f"Moving successfully forwarded email ID {msg_id} to '{processed_o365_folder.name}'.")
                        msg_obj.move(processed_o365_folder)
                    # Email successfully processed and handled.
                else:
                    # Forwarding FAILED
                    logger.error(f"Failed to forward email for privateid '{matched_privateid}'. Email ID: {msg_id}. Email will be left unread in current folder.")
                    # IMPORTANT: Do nothing else to the email (no mark_as_read, no move).
                    # The loop will continue to the next email, leaving this one as is.
            else:
                # Not a candidate for forwarding (e.g., no subject match, or match but no contacts, or no subject pattern defined)
                # This email should be moved to manual review if configured.
                if manual_review_o365_folder:
                    logger.info(f"Email (Subject: '{msg_subject}') not forwarded. Moving to '{manual_review_o365_folder.name}'.")
#                    logger.info(f"Email ID {msg_id} (Subject: '{msg_subject}') not forwarded. Moving to '{manual_review_o365_folder.name}'.")
                    msg_obj.mark_as_read()
                    msg_obj.move(manual_review_o365_folder)
                    manual_review_count += 1
                else:
                    # No manual review folder, and not forwarded (and not a forwarding failure).
                    # This is the "catch-all" case where the original script would just mark as read.
                    logger.info(f"Email (Subject: '{msg_subject}') not forwarded and no manual review folder. Marking as read.")
#                    logger.info(f"Email ID {msg_id} (Subject: '{msg_subject}') not forwarded and no manual review folder. Marking as read.")
                    try:
                        msg_obj.mark_as_read()
                    except Exception as e_mark_read:
                        logger.error(f"Failed to mark email as read in catch-all: {e_mark_read}")
#                        logger.error(f"Failed to mark email ID {msg_id} as read in catch-all: {e_mark_read}")

    except ConnectionRefusedError:
        # This specific error is less likely with Graph API (HTTP-based)
        logger.error(f"Connection refused. This is unusual for Graph API. Check network connectivity and Graph API endpoints.")
    except Exception as e:
        logger.critical(f"An error occurred during mailbox processing: {e}", exc_info=True)
        return False # Signal failure
    finally:
        logger.info(f"Email processing finished. Total emails checked: {processed_count}, Forwarded: {forwarded_count}, Moved to manual review: {manual_review_count}")
    return True # Signal success if we reach here

if __name__ == "__main__":
    if not process_mailbox():
        sys.exit(1)
