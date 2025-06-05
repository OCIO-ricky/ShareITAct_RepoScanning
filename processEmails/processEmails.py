# processEmails/processEmails.py
#
# Make sure your privateid_mappings.csv file is in the /output/ folder 
# (or update the PRIVATEID_MAPPINGS_CSV_PATH in your .env file to its correct location).
#
# $> cd processMails
# $> python process_email_requests.py
#
# Test thoroughly in a development environment.
# 
import os
import csv
import re
import smtplib
import logging
import time
from datetime import datetime
import base64 # For SMTP XOAUTH2
from dotenv import load_dotenv
from imap_tools import MailBox, AND, MailMessageFlags, MailBoxAuthType
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders, utils as email_utils

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# Load environment variables from .env file
load_dotenv()

# --- Environment Variable Retrieval and Validation ---
def get_env_var(var_name, is_required=True, default=None):
    """Retrieves an environment variable, with optional requirement and default."""
    value = os.getenv(var_name)
    if is_required and not value:
        logger.error(f"Missing required environment variable: {var_name}")
        raise ValueError(f"Missing required environment variable: {var_name}")
    return value if value else default

try:
    # IMAP Settings
    OUTLOOK_IMAP_SERVER = get_env_var("OUTLOOK_IMAP_SERVER")
    OUTLOOK_IMAP_PORT = int(get_env_var("OUTLOOK_IMAP_PORT", default="993"))
    OUTLOOK_EMAIL_ADDRESS = get_env_var("OUTLOOK_EMAIL_ADDRESS")

    # SMTP Settings
    OUTLOOK_SMTP_SERVER = get_env_var("OUTLOOK_SMTP_SERVER")
    OUTLOOK_SMTP_PORT = int(get_env_var("OUTLOOK_SMTP_PORT", default="587"))

    # Azure AD OAuth Settings
    AZURE_CLIENT_ID = get_env_var("AZURE_CLIENT_ID")
    AZURE_TENANT_ID = get_env_var("AZURE_TENANT_ID")
    AZURE_CLIENT_SECRET = get_env_var("AZURE_CLIENT_SECRET")
    OAUTH_SCOPE = get_env_var("OAUTH_SCOPE", default="https://outlook.office365.com/.default")

    # File and Folder Settings
    PRIVATEID_MAPPINGS_CSV_PATH = get_env_var("PRIVATEID_MAPPINGS_CSV_PATH", default="output/privateid_mappings.csv")
    IMAP_MAILBOX_TO_CHECK = get_env_var("IMAP_MAILBOX_TO_CHECK", default="INBOX")
    IMAP_PROCESSED_FOLDER = get_env_var("IMAP_PROCESSED_FOLDER", is_required=False) 
    IMAP_MANUAL_REVIEW_FOLDER = get_env_var("IMAP_MANUAL_REVIEW_FOLDER", is_required=False)
    TARGET_SUBJECT = get_env_var("TARGET_SUBJECT", is_required=False, default="").strip()


except ValueError as e:
    logger.critical(f"Configuration error: {e}. Exiting.")
    exit(1)

# --- OAuth Token Acquisition ---
def get_oauth_token() -> str | None:
    """Acquires an OAuth 2.0 access token from Azure AD."""
    try:
        import msal 
    except ImportError:
        logger.error("MSAL library is not installed. Please install it: pip install msal")
        return None

    authority = f"https://login.microsoftonline.com/{AZURE_TENANT_ID}"
    app = msal.ConfidentialClientApplication(
        AZURE_CLIENT_ID,
        authority=authority,
        client_credential=AZURE_CLIENT_SECRET,
    )

    result = app.acquire_token_for_client(scopes=[OAUTH_SCOPE])

    if "access_token" in result:
        logger.info("Successfully acquired OAuth token.")
        return result["access_token"]
    else:
        logger.error(f"Failed to acquire OAuth token: {result.get('error_description', result)}")
        return None

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
                
                if not emails and contact_emails_col_name: # Log if ContactEmails column exists but no valid emails found for this ID
                    logger.warning(f"No contact emails found for PrivateID '{private_id}' in row {row_num+1} of {csv_path}.")


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

def find_privateid_in_subject(subject: str, privateid_keys: list) -> str | None:
    """
    Finds a known privateid in the email subject.
    Returns the matched privateid (key from mappings) or None.
    """
    if not subject:
        return None
    for pid_key in privateid_keys:
        if re.search(r'\b' + re.escape(pid_key) + r'\b', subject, re.IGNORECASE):
            return pid_key
    return None

def forward_email_message(original_msg_obj, to_emails: list, from_email: str, subject_prefix="Fwd: "):
    """
    Forwards the original email message object to the specified recipients.
    original_msg_obj is an email.message.Message object.
    """
    if not to_emails:
        logger.warning("No recipient emails provided for forwarding.")
        return False

    forward_msg = MIMEMultipart()
    forward_msg['From'] = from_email
    forward_msg['To'] = ", ".join(to_emails)
    forward_msg['Date'] = email_utils.formatdate(localtime=True)

    original_subject = original_msg_obj.get('subject', "No Subject")
    forward_msg['Subject'] = f"{subject_prefix}{original_subject}"

    intro_text = (
        f"Hello,\n\nThe following email request regarding '{original_subject}' "
        f"(received from: {original_msg_obj.get('from', 'Unknown Sender')}) "
        "is being forwarded for your attention.\n\n"
        "Regards,\nShareIT Auto-Processor\n\n"
        "--- Original Message Below ---"
    )
    forward_msg.attach(MIMEText(intro_text, 'plain'))

    original_email_bytes = original_msg_obj.as_bytes()
    eml_attachment = MIMEBase('message', 'rfc822')
    eml_attachment.set_payload(original_email_bytes)
    encoders.encode_noop(eml_attachment) 
    eml_attachment.add_header('Content-Disposition', 'attachment; filename="forwarded_request.eml"')
    forward_msg.attach(eml_attachment)

    try:
        access_token = get_oauth_token()
        if not access_token:
            logger.error("SMTP: Failed to get OAuth token for forwarding.")
            return False

        with smtplib.SMTP(OUTLOOK_SMTP_SERVER, OUTLOOK_SMTP_PORT) as server:
            server.set_debuglevel(0) 
            server.ehlo()
            server.starttls()
            server.ehlo()

            auth_string = f"user={from_email}\x01auth=Bearer {access_token}\x01\x01"
            server.docmd('AUTH', f'XOAUTH2 {base64.b64encode(auth_string.encode()).decode()}')

            server.sendmail(from_email, to_emails, forward_msg.as_string())
        logger.info(f"Successfully forwarded email (Subject: {original_subject}) to: {', '.join(to_emails)}")
        return True
    except smtplib.SMTPAuthenticationError:
        logger.error("SMTP authentication failed. Check OAuth credentials/permissions or token.")
    except smtplib.SMTPException as e:
        logger.error(f"SMTP error while forwarding email: {e}")
    except Exception as e:
        logger.error(f"Unexpected error during email forwarding: {e}")
    return False

def ensure_mailbox_folder_exists(mb_client, folder_name):
    """Checks if a mailbox folder exists, and creates it if not."""
    if not folder_name:
        return
    try:
        if not mb_client.folder.exists(folder_name):
            logger.info(f"Folder '{folder_name}' does not exist. Creating it.")
            mb_client.folder.create(folder_name)
        else:
            logger.debug(f"Folder '{folder_name}' already exists.")
    except Exception as e:
        logger.error(f"Could not create or verify folder '{folder_name}': {e}")


# --- Main Processing Logic ---
def process_mailbox():
    logger.info("Starting email processing...")
    try:
        privateid_mappings = load_privateid_mappings(PRIVATEID_MAPPINGS_CSV_PATH)
    except Exception:
        logger.critical("Failed to load private ID mappings. Cannot proceed.")
        return

    if not privateid_mappings:
        logger.warning("No private ID mappings loaded. No emails will be processed for forwarding.")
        return

    processed_count = 0
    forwarded_count = 0
    manual_review_count = 0
    
    target_subject_regex = None
    if TARGET_SUBJECT:
        try:
            pattern_str = re.escape(TARGET_SUBJECT).replace(re.escape(r'[privateid]'), r'(\S+?)')
            target_subject_regex = re.compile(f"^{pattern_str}$", re.IGNORECASE) 
            logger.info(f"Using target subject pattern: {target_subject_regex.pattern}")
        except re.error as e:
            logger.error(f"Invalid regex pattern derived from TARGET_SUBJECT '{TARGET_SUBJECT}': {e}. Will not use target subject matching.")

    try:
        access_token = get_oauth_token()
        if not access_token:
            logger.critical("IMAP: Failed to get OAuth token. Cannot connect to mailbox.")
            return

        auth_bytes = (f"user={OUTLOOK_EMAIL_ADDRESS}\x01"
                      f"auth=Bearer {access_token}\x01\x01").encode()

        with MailBox(OUTLOOK_IMAP_SERVER, port=OUTLOOK_IMAP_PORT) as mailbox:
            mailbox.login(OUTLOOK_EMAIL_ADDRESS, auth_bytes, initial_folder=IMAP_MAILBOX_TO_CHECK, auth_type=MailBoxAuthType.XOAUTH2)
            logger.info(f"Successfully connected to IMAP server via OAuth. Checking folder '{IMAP_MAILBOX_TO_CHECK}'.")

            ensure_mailbox_folder_exists(mailbox, IMAP_PROCESSED_FOLDER)
            ensure_mailbox_folder_exists(mailbox, IMAP_MANUAL_REVIEW_FOLDER)

            fetch_criteria = AND(seen=False)
            logger.info(f"Fetching emails with criteria: {fetch_criteria}")

            emails_to_process = list(mailbox.fetch(criteria=fetch_criteria, mark_seen=False, bulk=True))
            logger.info(f"Found {len(emails_to_process)} email(s) matching criteria.")

            if not emails_to_process:
                logger.info("No new emails to process.")
                return

            for msg in emails_to_process:
                logger.info(f"Processing email UID {msg.uid} - Subject: '{msg.subject}' From: '{msg.from_}' Date: '{msg.date_str}'")
                processed_count += 1
                action_taken = False
                matched_privateid = None
                email_subject = msg.subject or ""

                if target_subject_regex:
                    match = target_subject_regex.search(email_subject)
                    if match:
                        potential_pid = match.group(1) 
                        if potential_pid in privateid_mappings:
                            matched_privateid = potential_pid
                            logger.info(f"Extracted privateid '{matched_privateid}' from subject using TARGET_SUBJECT pattern.")
                        else:
                            logger.warning(f"Subject matched TARGET_SUBJECT pattern, but extracted ID '{potential_pid}' not in mappings. Email UID: {msg.uid}, Subject: '{email_subject}'")
                    else:
                        logger.info(f"Email subject '{email_subject}' did not match TARGET_SUBJECT pattern. Email UID: {msg.uid}")
                else:
                    matched_privateid = find_privateid_in_subject(email_subject, list(privateid_mappings.keys()))
                    if matched_privateid:
                        logger.info(f"Found matching privateid '{matched_privateid}' in subject using general search.")
                
                if matched_privateid:
                    contact_emails = privateid_mappings.get(matched_privateid, [])
                    if contact_emails:
                        logger.info(f"Contact emails for '{matched_privateid}': {contact_emails}")
                        if forward_email_message(msg.obj, contact_emails, OUTLOOK_EMAIL_ADDRESS):
                            forwarded_count += 1
                            action_taken = True
                            mailbox.flag(msg.uid, [MailMessageFlags.SEEN], True)
                            if IMAP_PROCESSED_FOLDER:
                                logger.info(f"Moving email UID {msg.uid} to '{IMAP_PROCESSED_FOLDER}'.")
                                mailbox.move(msg.uid, IMAP_PROCESSED_FOLDER)
                        else:
                            logger.error(f"Failed to forward email for privateid '{matched_privateid}'. Email UID: {msg.uid}")
                            if IMAP_MANUAL_REVIEW_FOLDER:
                                mailbox.flag(msg.uid, [MailMessageFlags.SEEN], True) # Mark seen before moving
                                mailbox.move(msg.uid, IMAP_MANUAL_REVIEW_FOLDER)
                                manual_review_count += 1
                                action_taken = True # Action was taken (attempted move)
                    else:
                        logger.warning(f"No contact emails configured for privateid '{matched_privateid}'. Email UID: {msg.uid}")
                        if IMAP_MANUAL_REVIEW_FOLDER:
                             mailbox.flag(msg.uid, [MailMessageFlags.SEEN], True)
                             mailbox.move(msg.uid, IMAP_MANUAL_REVIEW_FOLDER)
                             manual_review_count += 1
                             action_taken = True
                else:
                    if not target_subject_regex or (target_subject_regex and not target_subject_regex.search(email_subject)):
                         # Log only if not using strict pattern or if strict pattern didn't match (already logged)
                        if not target_subject_regex:
                            logger.info(f"No known privateid found in subject of email UID {msg.uid} ('{email_subject}').")
                    # Always move to manual review if no match or other issue if folder is configured
                    if IMAP_MANUAL_REVIEW_FOLDER:
                        mailbox.flag(msg.uid, [MailMessageFlags.SEEN], True)
                        mailbox.move(msg.uid, IMAP_MANUAL_REVIEW_FOLDER)
                        manual_review_count += 1
                        action_taken = True

                if not action_taken: 
                    mailbox.flag(msg.uid, [MailMessageFlags.SEEN], True)
                    logger.debug(f"Marked email UID {msg.uid} as SEEN (no other action).")

    except ConnectionRefusedError:
        logger.error(f"IMAP connection refused. Check server details ({OUTLOOK_IMAP_SERVER}:{OUTLOOK_IMAP_PORT}) and network.")
    except Exception as e:
        logger.critical(f"An error occurred during mailbox processing: {e}", exc_info=True)
    finally:
        logger.info(f"Email processing finished. Total emails checked: {processed_count}, Forwarded: {forwarded_count}, Moved to manual review: {manual_review_count}")

if __name__ == "__main__":
    process_mailbox()
