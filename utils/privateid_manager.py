# utils/privateid_manager.py
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

class PrivateIdManager:
    """Manages the mapping between repositories and unique private IDs."""
    EXPECTED_HEADER = ["PrivateID", "RepositoryName", "Organization", "ContactEmails", "DateAdded"]

    def __init__(self, filepath="output/privateid_mapping.csv"):
        self.filepath = filepath
        # This now holds the single source of truth, loaded initially and updated during run
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
                        private_id = row.get('PrivateID', '').strip()
                        repo_name = row.get('RepositoryName', '').strip()
                        org_name = row.get('Organization', '').strip()
                        emails_str = row.get('ContactEmails', '')
                        date_added = row.get('DateAdded', '')

                        if not private_id or not repo_name or not org_name:
                             self.logger.warning(f"Skipping row {row_num} in {self.filepath}: Missing required field(s). Row: {row}")
                             continue

                        # Store emails as a sorted list internally
                        contact_emails = sorted(list(set(email.strip() for email in emails_str.split(';') if email.strip())))

                        key = (org_name.lower(), repo_name.lower())
                        self.mappings[key] = {
                            'id': private_id,
                            'repo': repo_name, # Store original case for writing
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


    def _generate_short_id(self, length=6):
        """Generates a random alphanumeric ID, checking for collisions."""
        characters = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
        max_retries = 100
        # Combine all known IDs (case-insensitive) for collision check
        used_ids_lower = {data['id'].lower() for data in self.mappings.values()}

        for _ in range(max_retries):
            new_id = ''.join(random.choice(characters) for _ in range(length))
            if new_id.lower() not in used_ids_lower:
                return new_id
            else:
                self.logger.debug(f"Generated short ID {new_id} collided, retrying...")
        self.logger.error(f"Failed to generate a unique short ID after {max_retries} retries.")
        # Fallback to ensure uniqueness if short ID fails
        # Ensure uuid is imported if using this fallback
        import uuid
        return str(uuid.uuid4())


    def get_or_generate_id(self, repo_name: str, organization: str, contact_emails: Optional[List[str]] = None) -> str:
        """
        Gets the existing PrivateID or generates a new one.
        Updates contact emails in the main mapping if necessary.
        """
        key = (organization.lower(), repo_name.lower())
        # Prepare new emails list (lowercase, sorted, unique)
        actual_emails_list = sorted(list(set(email.lower() for email in contact_emails if email))) if contact_emails else []

        if key in self.mappings:
            # Existing mapping found
            existing_data = self.mappings[key]
            existing_emails_list = existing_data.get('emails', [])

            # Check if emails need updating
            if actual_emails_list and actual_emails_list != existing_emails_list:
                log_level = logging.INFO if not existing_emails_list else logging.WARNING
                self.logger.log(log_level, f"Updating contact emails for existing PrivateID {existing_data['id']} ({organization}/{repo_name}). New: {';'.join(actual_emails_list)}")
                # --- Directly update the main mapping ---
                existing_data['emails'] = actual_emails_list
                self.updated_email_count += 1 # Increment update counter
                # --- No need to add to new_mappings ---

            return existing_data['id']
        else:
            # Generate new ID and add to main mapping
            new_id = self._generate_short_id()
            date_added = datetime.now(timezone.utc).isoformat()
            emails_str_log = ";".join(actual_emails_list) if actual_emails_list else ""

            # Log generation (DEBUG level)
            self.logger.debug(f"Generating new PrivateID '{new_id}' for {organization}/{repo_name}. Contacts: '{emails_str_log}'")

            # --- Add directly to the main mapping ---
            self.mappings[key] = {
                'id': new_id,
                'repo': repo_name, # Store original case
                'org': organization, # Store original case
                'emails': actual_emails_list, # Store as list
                'date': date_added
            }
            self.new_id_count += 1 # Increment new counter
            return new_id

    def get_contact_email_for_json(self, organization: str, repo_name: str, is_private: bool) -> Optional[str]:
        """
        Returns the appropriate contact email string for the code.json.
        - Public repos: Returns the first found actual email (alphabetically sorted) or None.
        - Private repos: Returns the generic email address.
        """
        if is_private:
            return PRIVATE_REPO_CONTACT_EMAIL  # i.e., shareit@cdc.gov
        else:
            key = (organization.lower(), repo_name.lower())
            if key in self.mappings:
                mapping_data = self.mappings[key]
                # Emails are already sorted in the mapping
                if mapping_data.get('emails'):
                    return mapping_data['emails'][0] # Return the first actual email
            return None # No mapping or no emails found for public repo

    # --- save_all_mappings ---
    def save_all_mappings(self):
        """Saves the entire current state of mappings to the CSV file, overwriting it."""
        if not self.mappings:
            self.logger.info("No private ID mappings exist in memory to save.")
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
                        data['id'],
                        data['repo'], 
                        data['org'],  
                        emails_str,
                        data['date']
                    ])
                    saved_count += 1
            self.logger.info(f"Successfully saved {saved_count} private ID mappings to {self.filepath}")
        except IOError as e:
            self.logger.error(f"Failed to save private ID mappings to {self.filepath}: {e}", exc_info=True)
        except Exception as e:
             self.logger.error(f"An unexpected error occurred while saving mappings: {e}", exc_info=True)

        # Reset counters after successful save
        self.new_id_count = 0
        self.updated_email_count = 0
    # --- END UPDATE ---

    def get_new_id_count(self) -> int:
        """Returns the count of *new* IDs generated since the last save."""
        return self.new_id_count

    def get_updated_email_count(self) -> int:
        """Returns the count of records where emails were updated since the last save."""
        return self.updated_email_count

# --- End PrivateIdManager ---
