# utils/privateid_manager.py
"""
Manages the mapping between private repositories and unique, anonymized IDs.

This module provides the `PrivateIdManager` class, responsible for generating,
storing, and retrieving persistent unique identifiers (PrivateIDs) for private
repositories. This mechanism is crucial for avoiding the exposure of potentially
sensitive repository names or URLs and direct contact emails in the public
`code.json` file.

The manager maintains a separate, non-public mapping file
(`output/privateid_mapping.csv` by default). This CSV file serves as the
authoritative link between a generated `PrivateID`, the original repository
details (name, repository URL, organization), and the actual contact email addresses extracted
during the scan.

Workflow for Private Repositories:
1.  When a private repository is processed, this manager assigns a unique `PrivateID`. 
    This is based on the platform name (e.g. GitHub) pluss the repo_id. 
2.  Any found contact emails are stored in the `privateid_mapping.csv` file for any given PrivateID.
3.  The public `code.json` file will list the `PrivateID` for each repository but display
    a generic contact email address (e.g., shareit@cdc.gov) instead of the real contact emails.
4.  External entities requesting access to a private repository's code will send
    their request to the generic email address listed in `code.json` and will reference the PrivateID.
5.  Internally, CDC can use the `privateid_mapping.csv` file to look up the
    `PrivateID` mentioned in the request and automatically redirect the inquiry
    to the correct internal contacts associated with that repository.
"""
import csv
import os
import uuid # Keep if using as fallback
import logging
from datetime import datetime, timezone
import random
import string
from typing import Optional, List, Dict, Any
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

logger = logging.getLogger(__name__)

class RepoIdMappingManager: # Renamed class
    """Manages the mapping between repositories (org/name) and their platform RepoIDs, URLs, and contacts."""
    EXPECTED_HEADER = ["PrivateID", "RepositoryName", "RepositoryURL","Organization", "ContactEmails", "DateAdded"] # Changed PlatformRepoID to PrivateID

    def __init__(self, filepath="output/privateid_mapping.csv"):
        self.filepath = filepath
        # Key: (org_lower, repo_name_lower), Value: dict with 'private_id', 'repo', 'org', 'url', 'emails', 'date'
        self.mappings: Dict[tuple[str, str], Dict[str, Any]] = {}
        # --- REMOVE new_mappings tracker ---
        # self.new_mappings = {}
        self.logger = logging.getLogger(__name__) # Instance logger if preferred
        self.new_id_count = 0 # Track genuinely new IDs generated
        self.updated_email_count = 0 # Track records where emails were updated
        self._ensure_csv_headers() # Ensure file/headers exist before loading
        self._load_mappings()

    def _ensure_csv_headers(self):
        """Ensure the CSV file exists and has the correct headers."""
        try:
            file_exists = os.path.isfile(self.filepath)
            # Check if file needs header (doesn't exist or is empty)
            needs_header = not file_exists or os.path.getsize(self.filepath) == 0
            if needs_header:
                # Ensure directory exists before writing
                os.makedirs(os.path.dirname(self.filepath), exist_ok=True)
                with open(self.filepath, 'w', newline='', encoding='utf-8') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow(self.EXPECTED_HEADER)
                self.logger.info(f"Created or initialized headers in {self.filepath}")
            elif file_exists: # Check header if file exists and is not empty
                 with open(self.filepath, 'r', newline='', encoding='utf-8') as csvfile:
                    reader = csv.reader(csvfile)
                    try:
                        header = next(reader)
                        if header != self.EXPECTED_HEADER:
                            self.logger.error(f"Header mismatch in {self.filepath}. Expected: {self.EXPECTED_HEADER}, Found: {header}. Please fix manually or delete the file.")
                            # Consider raising an error to stop execution
                            # raise ValueError(f"Header mismatch in {self.filepath}")
                    except StopIteration: # File exists but is empty after all
                         self.logger.warning(f"File {self.filepath} exists but is empty. Headers will be written if needed by save.")
                         # Re-create with header just in case
                         with open(self.filepath, 'w', newline='', encoding='utf-8') as csvfile_fix:
                              writer = csv.writer(csvfile_fix)
                              writer.writerow(self.EXPECTED_HEADER)

        except IOError as e:
            self.logger.error(f"Error ensuring CSV headers for {self.filepath}: {e}", exc_info=True)
            raise # Re-raise critical error

    def _load_mappings(self):
        """Loads existing mappings from the CSV file into self.mappings."""
        if not os.path.isfile(self.filepath) or os.path.getsize(self.filepath) == 0:
            self.logger.info(f"Mapping file {self.filepath} not found or empty. Starting fresh.")
            return # Nothing to load

        try:
            with open(self.filepath, mode='r', newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                # Check header directly from DictReader fieldnames
                if reader.fieldnames != self.EXPECTED_HEADER:
                     self.logger.error(f"Header mismatch loading mappings from {self.filepath}. Expected: {self.EXPECTED_HEADER}, Found: {reader.fieldnames}. Cannot load.")
                     return # Stop loading if headers don't match

                loaded_count = 0
                for row_num, row in enumerate(reader, start=2): # Start row count after header
                    try:
                        # Use .get with defaults for robustness
                        private_id_val = row.get('PrivateID', '').strip() # Changed from PlatformRepoID
                        repo_name = row.get('RepositoryName', '').strip()
                        repo_url = row.get('RepositoryURL', '').strip()
                        org_name = row.get('Organization', '').strip()
                        emails_str = row.get('ContactEmails', '') # Default to empty string if key missing
                        date_added = row.get('DateAdded', '')

                        if not private_id_val or not repo_name or not org_name or not repo_url:
                             self.logger.warning(f"Skipping row {row_num} in {self.filepath}: Missing required field(s). Row: {row}")
                             continue

                        # Store emails as a sorted list internally
                        contact_emails = sorted(list(set(email.strip() for email in emails_str.split(';') if email.strip())))

                        key = (org_name.lower(), repo_name.lower())
                        self.mappings[key] = {
                            'private_id': private_id_val, # Store the prefixed ID
                            'repo': repo_name, # Store original case for writing
                            'url': repo_url,
                            'org': org_name,   # Store original case for writing
                            'emails': contact_emails, # Store as list
                            'date': date_added
                        }
                        loaded_count += 1
                    except Exception as row_err:
                         self.logger.warning(f"Error processing row {row_num} in {self.filepath}: {row_err}. Row: {row}")

            self.logger.info(f"Loaded {loaded_count} existing private ID mappings from {self.filepath}")

        except FileNotFoundError:
             self.logger.warning(f"Mapping file {self.filepath} not found during load. Starting fresh.") # Should be caught earlier
        except Exception as e:
            self.logger.error(f"Error loading private ID mappings from {self.filepath}: {e}", exc_info=True)

    def get_or_create_mapping_entry(self, private_id_value: str, repo_name: str, organization: str, repository_url: str, contact_emails: Optional[List[str]] = None) -> str:
        """
        Ensures a mapping entry exists for the given private_id_value (prefixed repo_id), repo_name, organization, and URL.
        Updates contact emails in the main mapping if necessary.
        Returns the private_id_value.
        """
        if not private_id_value:
            self.logger.error(f"PrivateID (prefixed repo_id) is missing for {organization}/{repo_name}. Cannot create mapping entry.")
            return "" # Or raise an error
        if not repository_url: # repository_url is also essential for the mapping file
            self.logger.warning(f"RepositoryURL is missing for {organization}/{repo_name} (PrivateID: {private_id_value}). Mapping entry will lack URL.")
            # repository_url = "" # Or handle as an error if it's mandatory for your CSV

        key = (organization.lower(), repo_name.lower())
        actual_emails_list = sorted(list(set(email.lower() for email in contact_emails if email))) if contact_emails else []

        if key in self.mappings:
            existing_data = self.mappings[key]

            if str(existing_data.get('private_id')) != str(private_id_value):
                self.logger.warning(
                    f"PrivateID (prefixed repo_id) mismatch for {organization}/{repo_name}. "
                    f"Existing: {existing_data.get('private_id')}, New: {private_id_value}. Updating to new PrivateID."
                )
                existing_data['private_id'] = str(private_id_value)
            if existing_data.get('url') != repository_url: # Check and update URL
                self.logger.info(f"Updating RepositoryURL for {organization}/{repo_name}. Old: {existing_data.get('url')}, New: {repository_url}")
                existing_data['url'] = repository_url

            existing_emails_list = existing_data.get('emails', [])
            if actual_emails_list and actual_emails_list != existing_emails_list:
                log_level = logging.INFO if not existing_emails_list else logging.WARNING
                self.logger.log(log_level, f"Updating contact emails for PrivateID {existing_data['private_id']} ({organization}/{repo_name}). New: {';'.join(actual_emails_list)}")
                existing_data['emails'] = actual_emails_list
                self.updated_email_count += 1
            return str(existing_data['private_id'])
        else:
            date_added = datetime.now(timezone.utc).isoformat()
            emails_str_log = ";".join(actual_emails_list) if actual_emails_list else ""
            self.logger.debug(f"Creating new mapping entry for PrivateID '{private_id_value}' ({organization}/{repo_name}) URL: {repository_url}. Contacts: '{emails_str_log}'")
            self.mappings[key] = {
                'private_id': str(private_id_value), # Store the prefixed ID
                'repo': repo_name, # Store original case
                'url': repository_url,
                'org': organization, # Store original case
                'emails': actual_emails_list, # Store as list
                'date': date_added
            }
            self.new_id_count += 1 # Increment new counter
            return str(private_id_value)

    def get_contact_email_for_code_json(self, organization: str, repo_name: str, is_private_or_internal: bool) -> Optional[str]:
        """
        Returns the appropriate contact email string for the code.json.
        - Public repos: Returns the first found actual email (alphabetically sorted) or None.
        - Private repos: Returns the generic email address.
        """
        if is_private_or_internal:
            return PRIVATE_REPO_CONTACT_EMAIL  # i.e., shareit@cdc.gov
        else:
            key = (organization.lower(), repo_name.lower())
            if key in self.mappings:
                mapping_data = self.mappings[key]
                # Emails are already sorted in the mapping
                if mapping_data.get('emails'):
                    return mapping_data['emails'][0] # Return the first actual email
            return None # No mapping or no emails found for public repo

    def save_all_mappings(self):
        """Saves the entire current state of mappings to the CSV file, overwriting it."""
        if not self.mappings:
            self.logger.info("No repo ID mappings exist in memory to save.")
            return

        saved_count = 0
        try:
            # Open in write mode ('w') to overwrite the file
            with open(self.filepath, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                # Write the header first
                writer.writerow(self.EXPECTED_HEADER)

                # Write all rows from the in-memory dictionary
                # Sort by org then repo for consistent output order (optional)
                sorted_keys = sorted(self.mappings.keys())
                for key in sorted_keys:
                    data = self.mappings[key]
                    # Convert list of emails back to semicolon-separated string for CSV
                    emails_str = ";".join(data.get('emails', []))
                    writer.writerow([
                        data['private_id'], # Write the prefixed ID
                        data['repo'], 
                        data['url'],
                        data['org'],  
                        emails_str,
                        data['date']
                    ])
                    saved_count += 1
            self.logger.info(f"Successfully saved {saved_count} repo ID mappings to {self.filepath}")
        except IOError as e:
            self.logger.error(f"Failed to save repo ID mappings to {self.filepath}: {e}", exc_info=True)
        except Exception as e:
             self.logger.error(f"An unexpected error occurred while saving mappings: {e}", exc_info=True)

        self.new_id_count = 0
        self.updated_email_count = 0

    def get_new_id_count(self) -> int:
        """Returns the count of *new* mapping entries created since the last save."""
        return self.new_id_count

    def get_updated_email_count(self) -> int:
        """Returns the count of records where emails were updated since the last save."""
        return self.updated_email_count
