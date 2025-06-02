# utils\privateid_manager.py
"""
Manages a persistent mapping between repository identifiers (organization, name)
and a generated PrivateID. This PrivateID is intended to be a stable,
non-public identifier for repositories, especially useful when dealing with
repositories that might be private or internal.

The mapping is stored in a CSV file and loaded into memory on initialization.
It handles:
- Generating new PrivateIDs for new repositories.
- Retrieving existing PrivateIDs for known repositories.
- Storing and updating associated metadata like repository URL and contact emails.
- Ensuring thread-safety for concurrent access to the mapping data.
- Prefixing platform repository IDs to create a more globally unique PrivateID.
"""
import csv
import os
import logging
from datetime import datetime, timezone
import random
import threading # Added for lock
import string
from typing import Optional, List, Dict, Any
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

logger = logging.getLogger(__name__)

ANSI_RED = "\x1b[31;1m"  # Bold Red
ANSI_RESET = "\x1b[0m"   # Reset to default color

class RepoIdMappingManager:
    """Manages the mapping between repositories (org/name) and their platform RepoIDs, URLs, and contacts."""
    EXPECTED_HEADER = ["PrivateID", "RepositoryName", "RepositoryURL","Organization", "ContactEmails", "DateAdded"]

    def __init__(self, filepath="output/privateid_mapping.csv"):
        self.filepath = filepath
        # Key: private_id_value (e.g., "github_12345")
        # Value: dict with 'private_id', 'repo', 'org', 'url', 'emails', 'date'
        self.mappings: Dict[str, Dict[str, Any]] = {}
        self.lock = threading.Lock()
        self.logger = logging.getLogger(__name__) # Use the module-level logger
        self.new_id_count = 0
        self.updated_email_count = 0
        self._load_mappings()

    def _generate_random_suffix(self, length=6) -> str:
        """Generates a random alphanumeric suffix."""
        return ''.join(random.choices(string.ascii_lowercase + string.digits, k=length))

    def _ensure_directory_exists(self):
        """Ensures the directory for the mapping file exists."""
        os.makedirs(os.path.dirname(self.filepath), exist_ok=True)

    def _load_mappings(self):
        """Loads existing mappings from the CSV file into memory."""
        self._ensure_directory_exists()
        with self.lock:
            if not os.path.exists(self.filepath):
                self.logger.info(f"Mapping file {self.filepath} not found. A new one will be created on save if needed.")
                return

            try:
                with open(self.filepath, 'r', newline='', encoding='utf-8') as csvfile:
                    reader = csv.reader(csvfile)
                    header = next(reader, None)

                    if not header:
                        self.logger.warning(f"Mapping file {self.filepath} is empty. Will proceed as if no mappings exist.")
                        return

                    if header != self.EXPECTED_HEADER:
                        self.logger.error(
                            f"Header mismatch in {self.filepath}. "
                            f"Expected: {self.EXPECTED_HEADER}, Found: {header}. "
                            "Cannot reliably load mappings. Please check or regenerate the file."
                        )
                        self.mappings.clear()
                        return

                    for row_num, row in enumerate(reader, start=2):
                        if len(row) != len(self.EXPECTED_HEADER):
                            self.logger.warning(f"Skipping malformed row {row_num} in {self.filepath}: Incorrect number of columns. Row: {row}")
                            continue

                        private_id_val, repo_name, repo_url, org_name, emails_str, date_added = row

                        if not private_id_val or not repo_name or not org_name or not repo_url:
                             self.logger.warning(f"Skipping row {row_num} in {self.filepath}: Missing required field(s) (PrivateID, RepositoryName, Organization, RepositoryURL). Row: {row}")
                             continue

                        if private_id_val in self.mappings:
                            self.logger.warning(f"Duplicate PrivateID '{private_id_val}' found in {self.filepath} at row {row_num}. Keeping the first encountered entry. Row: {row}")
                            continue

                        # Parse and store emails as a sorted list of unique, lowercase strings
                        contact_emails_list = []
                        if emails_str: # Check if emails_str is not empty
                            contact_emails_list = sorted(list(set(
                                email.strip().lower() for email in emails_str.split(';') if email.strip()
                            )))

                        self.mappings[private_id_val] = {
                            'private_id': private_id_val,
                            'repo': repo_name,
                            'url': repo_url,
                            'org': org_name,
                            'emails': contact_emails_list, # Store the parsed list
                            'date': date_added
                        }
                self.logger.info(f"Successfully loaded {len(self.mappings)} repo ID mappings from {self.filepath}")
            except FileNotFoundError:
                self.logger.info(f"Mapping file {self.filepath} not found. Will create a new one on save if needed.")
            except Exception as e:
                self.logger.error(f"Error loading repo ID mappings from {self.filepath}: {e}", exc_info=True)
                self.mappings.clear()

    def get_or_create_mapping_entry(self, platform_repo_id: Any, organization: str, repo_name: str, repository_url: Optional[str], contact_emails_str_arg: Optional[str], platform_prefix: str) -> str:
        """
        Gets an existing PrivateID for a repo or creates a new one.
        Updates URL, organization, and contact emails if the entry exists and they differ.
        contact_emails_str_arg: A semicolon-separated string of contact emails, or None.
        """
        org_group_context_for_log = f"{organization}/{repo_name}"
        if not platform_repo_id:
            self.logger.info(f"get_or_create_mapping_entry: platform_repo_id is MISSING for {organization}/{repo_name}. Generating random suffix for PrivateID.", extra={'org_group': org_group_context_for_log})
            self.logger.error(f"Platform Repo ID is missing for {organization}/{repo_name}. Cannot generate PrivateID. Using random suffix.", extra={'org_group': org_group_context_for_log})
            private_id_value = f"{platform_prefix}_random_{self._generate_random_suffix()}"
        else:
            private_id_value = f"{platform_prefix}_{str(platform_repo_id)}"

        if not repository_url:
            self.logger.warning(f"RepositoryURL is missing for {organization}/{repo_name} (PrivateID: {private_id_value}). Mapping entry will lack URL.", extra={'org_group': org_group_context_for_log})

     #   self.logger.info(f"get_or_create_mapping_entry CALLED. PrivateID to check/create: '{private_id_value}' for repo: {organization}/{repo_name}. Incoming Org: '{organization}'. Emails str: '{contact_emails_str_arg}'")

        # Parse the incoming semicolon-separated string into a list of unique, sorted, lowercase emails.
        parsed_incoming_emails_list = []
        if contact_emails_str_arg and isinstance(contact_emails_str_arg, str): # Ensure it's a string
            parsed_incoming_emails_list = sorted(list(set(
                email.strip().lower() for email in contact_emails_str_arg.split(';') if email.strip()
            )))
        elif contact_emails_str_arg: # Log if it's not a string but also not None/empty
             self.logger.warning(f"contact_emails_str_arg for {private_id_value} was not a string: {type(contact_emails_str_arg)}. Treating as no emails.", extra={'org_group': org_group_context_for_log})
             # parsed_incoming_emails_list remains []

        with self.lock:
            if private_id_value in self.mappings:
                existing_data = self.mappings[private_id_value]
           #     self.logger.info(f"{ANSI_RED}PRIVATEID_MANAGER - Checking existing entry for {private_id_value}. Current in-memory: {existing_data}{ANSI_RESET}")
                updated = False

                # Organization update logic
                current_org_in_mapping = existing_data.get('org')
                org_needs_update = current_org_in_mapping != organization
           #     self.logger.info(f"{ANSI_RED}PRIVATEID_MANAGER - Org check for {private_id_value}: Incoming='{organization}', Existing='{current_org_in_mapping}', NeedsUpdate={org_needs_update}{ANSI_RESET}")
                if org_needs_update:
                    self.logger.debug(f"Updating Organization for PrivateID {private_id_value}. Old: '{current_org_in_mapping}', New: '{organization}'.", extra={'org_group': org_group_context_for_log})
                    existing_data['org'] = organization
                    updated = True
                
                # RepositoryURL update logic
                current_url_in_mapping = existing_data.get('url')
                url_needs_update = current_url_in_mapping != repository_url
            #    self.logger.info(f"{ANSI_RED}PRIVATEID_MANAGER - URL check for {private_id_value}: Incoming='{repository_url}', Existing='{current_url_in_mapping}', NeedsUpdate={url_needs_update}{ANSI_RESET}")
                if url_needs_update:
                    self.logger.debug(f"Updating RepositoryURL for PrivateID {private_id_value}. Old: '{current_url_in_mapping}', New: '{repository_url}'.", extra={'org_group': org_group_context_for_log})
                    existing_data['url'] = repository_url
                    updated = True

                # Repository Name update logic
                current_repo_name_in_mapping = existing_data.get('repo')
                repo_name_needs_update = current_repo_name_in_mapping != repo_name
            #    self.logger.info(f"{ANSI_RED}PRIVATEID_MANAGER - RepoName check for {private_id_value}: Incoming='{repo_name}', Existing='{current_repo_name_in_mapping}', NeedsUpdate={repo_name_needs_update}{ANSI_RESET}")
                if repo_name_needs_update:
                    self.logger.debug(f"Updating RepositoryName for PrivateID {private_id_value}. Old: '{current_repo_name_in_mapping}', New: '{repo_name}'.", extra={'org_group': org_group_context_for_log})
                    existing_data['repo'] = repo_name
                    updated = True

                # Contact Emails update logic
                existing_emails_list = existing_data.get('emails', [])
                emails_need_update = parsed_incoming_emails_list != existing_emails_list
            #    self.logger.info(f"{ANSI_RED}PRIVATEID_MANAGER - Emails check for {private_id_value}: ParsedIncoming={parsed_incoming_emails_list}, ExistingLoaded={existing_emails_list}, NeedsUpdate={emails_need_update}{ANSI_RESET}")
                if emails_need_update:
                    self.logger.debug(f"Updating contact emails for PrivateID {private_id_value}. Old: {';'.join(existing_emails_list)}, New: {';'.join(parsed_incoming_emails_list)}", extra={'org_group': org_group_context_for_log})
                    existing_data['emails'] = parsed_incoming_emails_list
                    self.updated_email_count += 1
                    updated = True
                
                if updated:
                    existing_data['date'] = datetime.now(timezone.utc).isoformat()
           #         self.logger.info(f"PRIVATEID_MANAGER - Entry for {private_id_value} was updated in memory. New state: {existing_data}")
           #     else:
           #         self.logger.info(f"PRIVATEID_MANAGER - Entry for {private_id_value} existed but no fields required updating.")

                return str(private_id_value)
            else: # New entry
                date_added = datetime.now(timezone.utc).isoformat()
                emails_str_log = ";".join(parsed_incoming_emails_list) if parsed_incoming_emails_list else ""
           #     self.logger.info(f"Attempting to ADD new entry to self.mappings with private_id_value='{private_id_value}' for {organization}/{repo_name}")
           #     self.logger.debug(f"Creating new mapping entry for PrivateID '{private_id_value}' ({organization}/{repo_name}) URL: {repository_url}. Contacts: '{emails_str_log}'")
                self.mappings[private_id_value] = {
                    'private_id': str(private_id_value),
                    'repo': repo_name,
                    'url': repository_url,
                    'org': organization,
                    'emails': parsed_incoming_emails_list, # Store the parsed list
                    'date': date_added
                }
                self.new_id_count += 1
           #     self.logger.info(f"SUCCESSFULLY ADDED new entry for private_id_value='{private_id_value}'. self.mappings now has {len(self.mappings)} entries.")
                return str(private_id_value)

    def get_contact_email_for_code_json(self, organization: str, repo_name: str, is_private_or_internal: bool) -> Optional[str]:
        """
        Determines the contact email for the code.json output.
        """
        if is_private_or_internal:
            return os.getenv("PRIVATE_REPO_CONTACT_EMAIL", "shareit@cdc.gov")
        else: # Public repository
            with self.lock:
                # Find the mapping entry by org/name to get its specific emails
                # This requires iterating as mappings are keyed by privateID
                for mapping_data in self.mappings.values():
                    if mapping_data.get('org', '').lower() == organization.lower() and \
                       mapping_data.get('repo', '').lower() == repo_name.lower():
                        if mapping_data.get('emails'): # Check if the 'emails' list exists and is not empty
                            return mapping_data['emails'][0] # Return the first actual email
            # If public and no specific emails found in mapping, return default public contact
            return os.getenv("DEFAULT_CONTACT_EMAIL", "shareit@cdc.gov")


    def save_all_mappings(self):
        """Saves all current mappings to the CSV file, overwriting it."""
        self._ensure_directory_exists()
        if not self.mappings:
            self.logger.info("No mappings in memory to save.")
            return

        saved_count = 0
        with self.lock:
            try:
                with open(self.filepath, 'w', newline='', encoding='utf-8') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow(self.EXPECTED_HEADER)

                    sorted_mapping_values = sorted(self.mappings.values(), key=lambda x: (x.get('org','').lower(), x.get('repo','').lower()))
                    for data in sorted_mapping_values:
                        emails_str = ";".join(data.get('emails', [])) # Convert list of emails back to string
                        writer.writerow([
                            data['private_id'],
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
        return self.new_id_count

    def get_updated_email_count(self) -> int:
        return self.updated_email_count
