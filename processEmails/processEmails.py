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
from datetime import datetime # Keep for email_utils.formatdate if used elsewhere, but not directly for auth
from dotenv import load_dotenv
from imap_tools import MailBox, AND, MailMessageFlags # MailBoxAuthType no longer needed for basic auth
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
    OUTLOOK_SERVICE_ACCOUNT_EMAIL_ADDRESS = get_env_var("OUTLOOK_SERVICE_ACCOUNT_EMAIL_ADDRESS") # For authentication
    OUTLOOK_SERVICE_ACCOUNT_PASSWORD = get_env_var("OUTLOOK_SERVICE_ACCOUNT_PASSWORD") # For authentication
    TARGET_MAILBOX_EMAIL_TO_SCAN = get_env_var("TARGET_MAILBOX_EMAIL_TO_SCAN", is_required=False, default="").strip() # Mailbox to actually scan

    # SMTP Settings
    OUTLOOK_SMTP_SERVER = get_env_var("OUTLOOK_SMTP_SERVER")
    OUTLOOK_SMTP_PORT = int(get_env_var("OUTLOOK_SMTP_PORT", default="587"))

    # File and Folder Settings
    PRIVATEID_MAPPINGS_CSV_PATH = get_env_var("PRIVATEID_MAPPINGS_CSV_PATH", default="output/privateid_mappings.csv")
    IMAP_MAILBOX_TO_CHECK = get_env_var("IMAP_MAILBOX_TO_CHECK", default="INBOX") # Folder name within the target mailbox
    IMAP_PROCESSED_FOLDER = get_env_var("IMAP_PROCESSED_FOLDER", is_required=False) # Folder name within the target mailbox
    IMAP_MANUAL_REVIEW_FOLDER = get_env_var("IMAP_MANUAL_REVIEW_FOLDER", is_required=False) # Folder name within the target mailbox
    TARGET_SUBJECT = get_env_var("TARGET_SUBJECT", is_required=False, default="").strip()


except ValueError as e:
    logger.critical(f"Configuration error: {e}. Exiting.")
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

    # Attach the original email as .eml
    original_email_bytes = original_msg_obj.as_bytes()
    eml_attachment = MIMEBase('message', 'rfc822')
    eml_attachment.set_payload(original_email_bytes)
    encoders.encode_noop(eml_attachment) # No actual encoding needed for message/rfc822
    eml_attachment.add_header('Content-Disposition', 'attachment; filename="forwarded_request.eml"')
    forward_msg.attach(eml_attachment)

    try:
        with smtplib.SMTP(OUTLOOK_SMTP_SERVER, OUTLOOK_SMTP_PORT) as server:
            server.set_debuglevel(0) # Set to 1 for verbose SMTP logs
            server.ehlo()
            server.starttls()
            server.ehlo()
            logger.info(f"SMTP: Attempting to login with user {OUTLOOK_SERVICE_ACCOUNT_EMAIL_ADDRESS}")
            # The 'from_email' here is OUTLOOK_SERVICE_ACCOUNT_EMAIL_ADDRESS, used for login
            server.login(OUTLOOK_SERVICE_ACCOUNT_EMAIL_ADDRESS, OUTLOOK_SERVICE_ACCOUNT_PASSWORD)
            logger.info(f"SMTP: Successfully logged in as {OUTLOOK_SERVICE_ACCOUNT_EMAIL_ADDRESS}")

            server.sendmail(from_email, to_emails, forward_msg.as_string())
        logger.info(f"Successfully forwarded email (Original Subject: {original_subject}) from {OUTLOOK_SERVICE_ACCOUNT_EMAIL_ADDRESS} to: {', '.join(to_emails)}")
        return True
    except smtplib.SMTPAuthenticationError:
        logger.error("SMTP authentication failed. Check service account credentials and SMTP permissions.")
    except smtplib.SMTPException as e:
        logger.error(f"SMTP error while forwarding email: {e}")
    except Exception as e:
        logger.error(f"Unexpected error during email forwarding: {e}")
    return False

def ensure_mailbox_folder_exists(mb_client, folder_full_path):
    """Checks if a mailbox folder exists, and creates it if not.
       folder_full_path is the complete path to the folder, e.g., 'shared@example.com\\Processed' or 'Processed'"""
    if not folder_full_path:
        return
    try:
        if not mb_client.folder.exists(folder_full_path):
            logger.info(f"Folder '{folder_full_path}' does not exist. Attempting to create it.")
            mb_client.folder.create(folder_full_path)
            logger.info(f"Successfully created folder '{folder_full_path}'.")
        else:
            logger.debug(f"Folder '{folder_full_path}' already exists.")
    except Exception as e:
        # Log the specific folder path that caused the error
        logger.error(f"Could not create or verify folder '{folder_full_path}': {e}")
        # Optionally, re-raise if this is critical, or handle gracefully
        # raise


# --- Main Processing Logic ---
def process_mailbox():
    logger.info("Starting email processing...")

    # --- Initial Login Check ---
    try:
        logger.info(f"Attempting initial IMAP login to {OUTLOOK_IMAP_SERVER} as {OUTLOOK_SERVICE_ACCOUNT_EMAIL_ADDRESS} to verify credentials...")
        with MailBox(OUTLOOK_IMAP_SERVER, port=OUTLOOK_IMAP_PORT) as test_mailbox:
            test_mailbox.login(OUTLOOK_SERVICE_ACCOUNT_EMAIL_ADDRESS, OUTLOOK_SERVICE_ACCOUNT_PASSWORD, initial_folder="INBOX")
        logger.info("Initial IMAP login successful. Credentials verified.")
    except Exception as e: # Catches MailboxLoginError and other connection issues
        logger.critical(f"Initial IMAP login failed: {e}. Please check credentials and server settings. Exiting.")
        return # Exit the function if login fails

    try:
        # --- Load Mappings (only if login was successful) ---
        privateid_mappings = load_privateid_mappings(PRIVATEID_MAPPINGS_CSV_PATH)
    except Exception:
        logger.critical("Failed to load private ID mappings. Cannot proceed.")
        return

    if not privateid_mappings:
        logger.warning("No private ID mappings loaded. No emails will be processed for forwarding.")
        # Still proceed to check emails to move them to manual review if configured
        # return # Uncomment this if you want to exit if no mappings are found

    processed_count = 0
    forwarded_count = 0
    manual_review_count = 0

    # --- Compile Regexes ---
    target_subject_regex = None
    general_search_pid_regex = None
    if TARGET_SUBJECT:
        try:
            # Escape the literal parts of TARGET_SUBJECT, then replace escaped [privateid] with regex group
            pattern_str = re.escape(TARGET_SUBJECT).replace(re.escape(r'[privateid]'), r'(\S+?)')
            target_subject_regex = re.compile(f"^{pattern_str}$", re.IGNORECASE)
            logger.info(f"Using target subject pattern: {target_subject_regex.pattern}")
        except re.error as e:
            logger.error(f"Invalid regex pattern derived from TARGET_SUBJECT '{TARGET_SUBJECT}': {e}. Will not use target subject matching.")
            target_subject_regex = None # Ensure it's None if compilation fails
    
    if privateid_mappings:
        # For general search: compile a single regex for all known PrivateIDs
        # Sort keys by length (descending) to match longer PIDs first if they are substrings of others,
        # though \b should largely prevent incorrect partial matches.
        sorted_pid_keys = sorted(list(privateid_mappings.keys()), key=len, reverse=True)
        if sorted_pid_keys: # Ensure there are keys to join
            pattern_str = r'\b(?:' + '|'.join(re.escape(pid) for pid in sorted_pid_keys) + r')\b'
            general_search_pid_regex = re.compile(pattern_str, re.IGNORECASE)
            logger.info(f"Compiled general PrivateID search regex for {len(sorted_pid_keys)} IDs.")

    try:
        # Determine the base path for mailbox operations
        # For Exchange/Outlook 365, paths to shared mailbox folders are often like "sharedmailbox@domain.com\FolderName"
        # The backslash is important for Exchange. imap-tools handles this.
        mailbox_prefix = ""
        effective_mailbox_target = OUTLOOK_SERVICE_ACCOUNT_EMAIL_ADDRESS # Default to service account's own mailbox
        if TARGET_MAILBOX_EMAIL_TO_SCAN:
            mailbox_prefix = f"{TARGET_MAILBOX_EMAIL_TO_SCAN}\\" # Note: imap-tools might use / or auto-detect
            effective_mailbox_target = TARGET_MAILBOX_EMAIL_TO_SCAN
            logger.info(f"Targeting shared/specific mailbox: {TARGET_MAILBOX_EMAIL_TO_SCAN}")
        else:
            logger.info(f"Targeting service account's own mailbox: {OUTLOOK_SERVICE_ACCOUNT_EMAIL_ADDRESS}")

        # Construct full paths for mailbox folders
        # IMAP_MAILBOX_TO_CHECK is relative to the target mailbox context
        folder_to_scan_full_path = f"{mailbox_prefix}{IMAP_MAILBOX_TO_CHECK}"

        processed_folder_full_path = None
        if IMAP_PROCESSED_FOLDER:
            processed_folder_full_path = f"{mailbox_prefix}{IMAP_PROCESSED_FOLDER}"

        manual_review_folder_full_path = None
        if IMAP_MANUAL_REVIEW_FOLDER:
            manual_review_folder_full_path = f"{mailbox_prefix}{IMAP_MANUAL_REVIEW_FOLDER}"


        with MailBox(OUTLOOK_IMAP_SERVER, port=OUTLOOK_IMAP_PORT) as mailbox:
            logger.info(f"IMAP: Attempting to login as {OUTLOOK_SERVICE_ACCOUNT_EMAIL_ADDRESS} to server {OUTLOOK_IMAP_SERVER}")
            # Login to the service account's own context first.
            # The initial_folder here refers to a folder in the service account's own mailbox.
            # It's often fine to use the default or the service account's INBOX.
            mailbox.login(OUTLOOK_SERVICE_ACCOUNT_EMAIL_ADDRESS, OUTLOOK_SERVICE_ACCOUNT_PASSWORD, initial_folder="INBOX")
            logger.info(f"Successfully authenticated as {OUTLOOK_SERVICE_ACCOUNT_EMAIL_ADDRESS}.")

            logger.info(f"Setting current folder to: '{folder_to_scan_full_path}' for mailbox {effective_mailbox_target}")
            try:
                mailbox.folder.set(folder_to_scan_full_path)
                logger.info(f"Successfully set folder to '{mailbox.folder.get()}'.")
            except Exception as e:
                logger.error(f"Failed to set IMAP folder to '{folder_to_scan_full_path}': {e}. Check folder name and permissions for {OUTLOOK_SERVICE_ACCOUNT_EMAIL_ADDRESS} on {effective_mailbox_target}.")
                return # Cannot proceed if folder cannot be set

            if processed_folder_full_path: ensure_mailbox_folder_exists(mailbox, processed_folder_full_path)
            if manual_review_folder_full_path: ensure_mailbox_folder_exists(mailbox, manual_review_folder_full_path)

            fetch_criteria = AND(seen=False)
            logger.info(f"Fetching emails from '{mailbox.folder.get()}' with criteria: {fetch_criteria}")

            emails_to_process = list(mailbox.fetch(criteria=fetch_criteria, mark_seen=False, bulk=True))
            logger.info(f"Found {len(emails_to_process)} email(s) matching criteria in '{mailbox.folder.get()}'.")

            if not emails_to_process:
                logger.info("No new emails to process.")
                return

            for msg in emails_to_process:
                logger.info(f"Processing email UID {msg.uid} - Subject: '{msg.subject}' From: '{msg.from_}' Date: '{msg.date_str}'")
                processed_count += 1
                action_taken = False # Flag to track if email was moved or attempted to be forwarded
                matched_privateid = None
                email_subject = msg.subject or ""

                if target_subject_regex:
                    match = target_subject_regex.search(email_subject)
                    if match:
                        potential_pid = match.group(1) # The captured group for [privateid]
                        if potential_pid in privateid_mappings:
                            matched_privateid = potential_pid
                            logger.info(f"Extracted privateid '{matched_privateid}' from subject using TARGET_SUBJECT pattern.")
                        else:
                            logger.warning(f"Subject matched TARGET_SUBJECT pattern, but extracted ID '{potential_pid}' not in mappings. Email UID: {msg.uid}, Subject: '{email_subject}'")
                    else:
                        logger.info(f"Email subject '{email_subject}' did not match TARGET_SUBJECT pattern. Email UID: {msg.uid}")
                
                # If no match from TARGET_SUBJECT or TARGET_SUBJECT is not set, try general search
                if not matched_privateid and general_search_pid_regex: # Check if regex was compiled
                    match_obj = general_search_pid_regex.search(email_subject)
                    if match_obj:
                        matched_privateid = match_obj.group(0) # The matched PID itself
                        logger.info(f"Found matching privateid '{matched_privateid}' in subject using general search regex.")

                if matched_privateid and privateid_mappings.get(matched_privateid): # Ensure PID exists and has contacts
                    contact_emails = privateid_mappings.get(matched_privateid, [])
                    if contact_emails: # This check is somewhat redundant due to the outer if, but safe
                        logger.info(f"Contact emails for '{matched_privateid}': {contact_emails}")
                        if forward_email_message(msg.obj, contact_emails, OUTLOOK_SERVICE_ACCOUNT_EMAIL_ADDRESS):
                            forwarded_count += 1
                            action_taken = True
                            mailbox.flag(msg.uid, [MailMessageFlags.SEEN], True)
                            if processed_folder_full_path:
                                logger.info(f"Moving email UID {msg.uid} to '{processed_folder_full_path}'.")
                                mailbox.move(msg.uid, processed_folder_full_path)
                        else:
                            logger.error(f"Failed to forward email for privateid '{matched_privateid}'. Email UID: {msg.uid}")
                            if manual_review_folder_full_path:
                                mailbox.flag(msg.uid, [MailMessageFlags.SEEN], True) # Mark seen before moving
                                mailbox.move(msg.uid, manual_review_folder_full_path)
                                manual_review_count += 1
                                action_taken = True # Action was taken (attempted move)
                    else: # Should not be reached if outer if is `privateid_mappings.get(matched_privateid)`
                        logger.warning(f"No contact emails configured for privateid '{matched_privateid}' (this should be rare if mappings loaded). Email UID: {msg.uid}")
                        if manual_review_folder_full_path:
                             mailbox.flag(msg.uid, [MailMessageFlags.SEEN], True)
                             mailbox.move(msg.uid, manual_review_folder_full_path)
                             manual_review_count += 1
                             action_taken = True
                else: # No valid PrivateID found or no contacts for it
                    if not target_subject_regex or (target_subject_regex and not target_subject_regex.search(email_subject)):
                        if not target_subject_regex and email_subject: # Log only if not using strict pattern or if strict pattern didn't match
                             logger.info(f"No known or extractable privateid found in subject of email UID {msg.uid} ('{email_subject}').")
                        elif not email_subject:
                             logger.info(f"Email UID {msg.uid} has no subject.")

                    if manual_review_folder_full_path:
                        mailbox.flag(msg.uid, [MailMessageFlags.SEEN], True)
                        mailbox.move(msg.uid, manual_review_folder_full_path)
                        manual_review_count += 1
                        action_taken = True

                if not action_taken: # If no move or forward attempt happened, just mark as seen
                    mailbox.flag(msg.uid, [MailMessageFlags.SEEN], True)
                    logger.debug(f"Marked email UID {msg.uid} as SEEN (no other specific action taken).")

    except ConnectionRefusedError:
        logger.error(f"IMAP connection refused. Check server details ({OUTLOOK_IMAP_SERVER}:{OUTLOOK_IMAP_PORT}) and network.")
    except smtplib.SMTPAuthenticationError: # This might be better caught in forward_email_message
        logger.error("SMTP authentication failed. This is unexpected here, check forward_email_message.")
    except Exception as e:
        logger.critical(f"An error occurred during mailbox processing: {e}", exc_info=True)
    finally:
        logger.info(f"Email processing finished. Total emails checked: {processed_count}, Forwarded: {forwarded_count}, Moved to manual review: {manual_review_count}")

if __name__ == "__main__":
    process_mailbox()
