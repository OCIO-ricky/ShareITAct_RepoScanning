# utils/privateid_manager.py
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

class RepoIdMappingManager: # Renamed class
    """Manages the mapping between repositories (org/name) and their platform RepoIDs, URLs, and contacts."""
    EXPECTED_HEADER = ["PrivateID", "RepositoryName", "RepositoryURL","Organization", "ContactEmails", "DateAdded"] # Changed PlatformRepoID to PrivateID

    def __init__(self, filepath="output/privateid_mapping.csv"):
        self.filepath = filepath
        # Key: private_id_value (e.g., "github_12345")
        # Value: dict with 'private_id', 'repo', 'org', 'url', 'emails', 'date'
        self.mappings: Dict[str, Dict[str, Any]] = {}
        self.lock = threading.Lock() # Initialize the lock
        self.logger = logging.getLogger(__name__) # Instance logger if preferred
        self.new_id_count = 0 # Track genuinely new IDs generated
        self.updated_email_count = 0 # Track records where emails were updated
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
        with self.lock: # Protect access to self.mappings during load
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
                        # Potentially raise an error or clear self.mappings to prevent using bad data
                        self.mappings.clear()
                        return

                    for row_num, row in enumerate(reader, start=2): # start=2 because header is row 1
                        if len(row) != len(self.EXPECTED_HEADER):
                            self.logger.warning(f"Skipping malformed row {row_num} in {self.filepath}: Incorrect number of columns. Row: {row}")
                            continue
                        
                        private_id_val, repo_name, repo_url, org_name, emails_str, date_added = row
                        
                        # Basic validation for required fields
                        if not private_id_val or not repo_name or not org_name or not repo_url:
                             self.logger.warning(f"Skipping row {row_num} in {self.filepath}: Missing required field(s) (PrivateID, RepositoryName, Organization, RepositoryURL). Row: {row}")
                             continue
                        
                        # Check for duplicate PrivateID during load
                        if private_id_val in self.mappings:
                            self.logger.warning(f"Duplicate PrivateID '{private_id_val}' found in {self.filepath} at row {row_num}. Keeping the first encountered entry. Row: {row}")
                            continue # Skip this duplicate
                            
                        # Store emails as a sorted list internally
                        contact_emails = sorted(list(set(email.strip() for email in emails_str.split(';') if email.strip())))

                        # Key is now private_id_val
                        self.mappings[private_id_val] = {
                            'private_id': private_id_val, # Store the prefixed ID
                            'repo': repo_name, # Store original case for writing
                            'url': repo_url,
                            'org': org_name,   # Store original case for writing
                            'emails': contact_emails, # Store as list
                            'date': date_added
                        }
                self.logger.info(f"Successfully loaded {len(self.mappings)} repo ID mappings from {self.filepath}")
            except FileNotFoundError:
                self.logger.info(f"Mapping file {self.filepath} not found. Will create a new one on save if needed.")
            except Exception as e:
                self.logger.error(f"Error loading repo ID mappings from {self.filepath}: {e}", exc_info=True)
                # Decide if to clear mappings or try to proceed with what was loaded
                self.mappings.clear() # Safer to clear if loading failed unexpectedly

    def get_or_create_mapping_entry(self, platform_repo_id: Any, organization: str, repo_name: str, repository_url: Optional[str], contact_emails: Optional[List[str]], platform_prefix: str) -> str:
        """
        Gets an existing PrivateID for a repo or creates a new one.
        Updates URL and contact emails if the entry exists and they differ.
        The platform_repo_id is the unique ID from the platform (e.g., GitHub repo.id).
        The platform_prefix is 'github', 'gitlab', or 'azure'.
        """
        if not platform_repo_id:
            self.logger.error(f"Platform Repo ID is missing for {organization}/{repo_name}. Cannot generate PrivateID. Using random suffix.")
            # Fallback to a less stable identifier if platform_repo_id is missing
            private_id_value = f"{platform_prefix}_random_{self._generate_random_suffix()}"
        else:
            private_id_value = f"{platform_prefix}_{str(platform_repo_id)}"

        if not repository_url:
            self.logger.warning(f"RepositoryURL is missing for {organization}/{repo_name} (PrivateID: {private_id_value}). Mapping entry will lack URL.")
            # repository_url = "" # Or handle as an error if it's mandatory for your CSV

        with self.lock: # Acquire lock
            actual_emails_list = sorted(list(set(email.lower() for email in contact_emails if email))) if contact_emails else []

            if private_id_value in self.mappings:
                existing_data = self.mappings[private_id_value]
                updated = False

                if existing_data.get('repo') != repo_name:
                    self.logger.info(f"Updating RepositoryName for PrivateID {private_id_value}. Old: '{existing_data.get('repo')}', New: '{repo_name}'.")
                    existing_data['repo'] = repo_name
                    updated = True
                if existing_data.get('org') != organization:
                    self.logger.info(f"Updating Organization for PrivateID {private_id_value}. Old: '{existing_data.get('org')}', New: '{organization}'.")
                    existing_data['org'] = organization
                    updated = True
                if existing_data.get('url') != repository_url:
                    self.logger.info(f"Updating RepositoryURL for PrivateID {private_id_value}. Old: '{existing_data.get('url')}', New: '{repository_url}'.")
                    existing_data['url'] = repository_url
                    updated = True

                existing_emails_list = existing_data.get('emails', [])
                if actual_emails_list != existing_emails_list: # Compare sorted lists
                    self.logger.info(f"Updating contact emails for PrivateID {private_id_value}. Old: {';'.join(existing_emails_list)}, New: {';'.join(actual_emails_list)}")
                    existing_data['emails'] = actual_emails_list
                    self.updated_email_count += 1 # Count email updates specifically
                    updated = True
                
                if updated:
                    existing_data['date'] = datetime.now(timezone.utc).isoformat() # Update modification date

                return str(private_id_value)
            else:
                date_added = datetime.now(timezone.utc).isoformat()
                emails_str_log = ";".join(actual_emails_list) if actual_emails_list else ""
                self.logger.debug(f"Creating new mapping entry for PrivateID '{private_id_value}' ({organization}/{repo_name}) URL: {repository_url}. Contacts: '{emails_str_log}'")
                self.mappings[private_id_value] = {
                    'private_id': str(private_id_value), # Store the prefixed ID
                    'repo': repo_name, # Store original case
                    'url': repository_url,
                    'org': organization, # Store original case
                    'emails': actual_emails_list, # Store as list
                    'date': date_added
                }
                self.new_id_count += 1 # Increment new counter
                return str(private_id_value)
        # Lock is released automatically

    def get_contact_email_for_code_json(self, organization: str, repo_name: str, is_private_or_internal: bool) -> Optional[str]:
        """
        Determines the contact email for the code.json output.
        - Private/Internal repos: Returns a configured default email.
        - Public repos: Returns the first contact email found in the mapping,
                        or a configured default public email if none found.
        """
        if is_private_or_internal:
            return os.getenv("PRIVATE_REPO_CONTACT_EMAIL", "shareit@cdc.gov") # Fetch from env
        else:
            # Iterate to find the matching org/repo since privateID is the key
            with self.lock: # Protect reading self.mappings
                for mapping_data in self.mappings.values():
                    if mapping_data.get('org', '').lower() == organization.lower() and \
                       mapping_data.get('repo', '').lower() == repo_name.lower():
                        if mapping_data.get('emails'):
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
        with self.lock: # Acquire lock for writing
            try:
                # Open in write mode ('w') to overwrite the file
                with open(self.filepath, 'w', newline='', encoding='utf-8') as csvfile:
                    writer = csv.writer(csvfile)
                    # Write the header first
                    writer.writerow(self.EXPECTED_HEADER)

                    # Sort by privateID for consistent output order (optional, but good practice)
                    # self.mappings.values() are the dictionaries we want to write
                    sorted_mapping_values = sorted(self.mappings.values(), key=lambda x: (x.get('org','').lower(), x.get('repo','').lower()))
                    for data in sorted_mapping_values:
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
        """Returns the count of new PrivateIDs generated during this run."""
        return self.new_id_count

    def get_updated_email_count(self) -> int:
        """Returns the count of records where contact emails were updated during this run."""
        return self.updated_email_count

if __name__ == '__main__':
    # Basic test and usage example
    logging.basicConfig(level=logging.DEBUG)
    logger.info("--- Testing RepoIdMappingManager ---")
    
    # Use a temporary file for testing
    test_mapping_file = "output/test_privateid_mapping.csv"
    if os.path.exists(test_mapping_file):
        os.remove(test_mapping_file)

    manager = RepoIdMappingManager(filepath=test_mapping_file)

    # Test 1: Create new entries
    id1 = manager.get_or_create_mapping_entry("gh_123", "TestOrg", "RepoA", "http://example.com/TestOrg/RepoA", ["dev1@example.com", "dev2@example.com"], "github")
    id2 = manager.get_or_create_mapping_entry("gl_456", "TestOrg", "RepoB", "http://example.com/TestOrg/RepoB", ["dev3@example.com"], "gitlab")
    id3 = manager.get_or_create_mapping_entry("az_789", "AnotherOrg", "RepoC", "http://example.com/AnotherOrg/RepoC", [], "azure")
    
    logger.info(f"Generated ID for RepoA: {id1}")
    logger.info(f"Generated ID for RepoB: {id2}")
    logger.info(f"Generated ID for RepoC: {id3}")
    assert id1 == "github_gh_123"
    assert manager.get_new_id_count() == 3

    # Test 2: Retrieve existing entry (should not create new)
    id1_retrieved = manager.get_or_create_mapping_entry("gh_123", "TestOrg", "RepoA", "http://example.com/TestOrg/RepoA", ["dev1@example.com", "dev2@example.com"], "github")
    assert id1_retrieved == id1
    assert manager.get_new_id_count() == 3 # Count should not increase

    # Test 3: Update existing entry's emails and URL
    id1_updated = manager.get_or_create_mapping_entry("gh_123", "TestOrg", "RepoA", "http://new.example.com/TestOrg/RepoA_renamed", ["dev1@example.com", "newdev@example.com"], "github")
    assert id1_updated == id1
    assert manager.get_new_id_count() == 3
    assert manager.get_updated_email_count() == 1
    
    # Check internal state for RepoA
    repo_a_data = manager.mappings.get(id1)
    assert repo_a_data is not None
    assert repo_a_data['url'] == "http://new.example.com/TestOrg/RepoA_renamed"
    assert repo_a_data['emails'] == sorted(["dev1@example.com", "newdev@example.com"])


    # Test 4: Save and reload
    manager.save_all_mappings()
    logger.info(f"New ID count before reload: {manager.get_new_id_count()}") # Should be 0 after save
    logger.info(f"Updated email count before reload: {manager.get_updated_email_count()}") # Should be 0 after save

    manager_reloaded = RepoIdMappingManager(filepath=test_mapping_file)
    assert len(manager_reloaded.mappings) == 3
    assert manager_reloaded.get_new_id_count() == 0 # Should be 0 after loading
    
    id1_reloaded = manager_reloaded.get_or_create_mapping_entry("gh_123", "TestOrg", "RepoA", "http://new.example.com/TestOrg/RepoA_renamed", ["dev1@example.com", "newdev@example.com"], "github")
    assert id1_reloaded == id1
    assert manager_reloaded.get_new_id_count() == 0 # Still 0, no new IDs

    # Test 5: Get contact email
    email_repo_a = manager_reloaded.get_contact_email_for_code_json("TestOrg", "RepoA", is_private_or_internal=False)
    assert email_repo_a == "dev1@example.com" # First sorted email

    email_repo_c_public = manager_reloaded.get_contact_email_for_code_json("AnotherOrg", "RepoC", is_private_or_internal=False)
    assert email_repo_c_public == os.getenv("DEFAULT_CONTACT_EMAIL", "shareit@cdc.gov") # No emails, public, so default public

    email_repo_c_private = manager_reloaded.get_contact_email_for_code_json("AnotherOrg", "RepoC", is_private_or_internal=True)
    assert email_repo_c_private == os.getenv("PRIVATE_REPO_CONTACT_EMAIL", "shareit@cdc.gov") # Private, so default private

    # Test 6: Duplicate PrivateID in CSV (manual simulation for loading robustness)
    if os.path.exists(test_mapping_file):
        with open(test_mapping_file, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            # Add a row with an existing PrivateID but different data
            writer.writerow(["github_gh_123", "RepoADuplicate", "http://duplicate.com", "TestOrg", "dup@example.com", datetime.now(timezone.utc).isoformat()])
    
    manager_dup_test = RepoIdMappingManager(filepath=test_mapping_file)
    assert len(manager_dup_test.mappings) == 3 # Should still be 3, duplicate PrivateID ignored
    repo_a_after_dup_load = manager_dup_test.mappings.get("github_gh_123")
    assert repo_a_after_dup_load is not None
    assert repo_a_after_dup_load['repo'] == "RepoA" # Should have original RepoA data, not RepoADuplicate

    logger.info("--- RepoIdMappingManager tests completed ---")
    # Clean up test file
    # if os.path.exists(test_mapping_file):
    #     os.remove(test_mapping_file)
