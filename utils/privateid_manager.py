# utils/privateid_manager.py
import csv
import os
import random
import logging

logger = logging.getLogger(__name__)

class PrivateIdManager:
    """Handles loading, generating, and saving private IDs for repositories."""

    def __init__(self, filepath, template_path="templates/privateid_mapping_template.csv"):
        """
        Initializes the PrivateIdManager.

        Args:
            filepath (str): Path to the private ID mapping CSV file.
            template_path (str): Path to the template CSV file.
        """
        self.filepath = filepath
        self.template_path = template_path
        # Maps (repo_name, organization) -> {privateID, contactEmails}
        self.mapping = {}
        self.existing_ids = set()
        # Counter for new IDs generated during this run
        self.new_ids_generated_count = 0
        # List to queue newly generated mappings before saving
        self.new_mappings_to_save = []
        # Ensure file exists with headers before loading
        self._ensure_file_with_headers()
        # Load existing data
        self._load_mapping()

    def _ensure_file_with_headers(self):
        """Ensures the mapping file exists and has the correct headers."""
        file_exists = os.path.isfile(self.filepath)
        headers_needed = False

        if not file_exists:
            # Create directory if needed
            # Use os.path.dirname to get the directory part of the filepath
            dir_path = os.path.dirname(self.filepath)
            # Ensure the directory path is not empty before creating
            if dir_path:
                 os.makedirs(dir_path, exist_ok=True)

            logger.info(f"Mapping file not found. Creating '{self.filepath}'.")
            # Try copying from template first
            try:
                with open(self.template_path, 'r', encoding='utf-8') as tpl, open(self.filepath, 'w', encoding='utf-8') as out:
                    # Check if template actually has content/headers
                    template_content = tpl.read()
                    if template_content.strip():
                        out.write(template_content)
                        logger.info(f"Created private ID mapping from template: {self.filepath}")
                        # Check if the copied template actually had headers, assume yes for now
                        # More robust check could parse the first line of template_content
                        file_exists = True # File now exists
                    else:
                        logger.warning(f"Template file '{self.template_path}' is empty. Will write headers directly.")
                        headers_needed = True # Need to write headers manually
            except FileNotFoundError:
                logger.warning(f"Template file not found at {self.template_path}. Will create mapping file with headers.")
                headers_needed = True # Need to write headers manually
            except Exception as e:
                 logger.error(f"Error copying template file '{self.template_path}': {e}. Will create mapping file with headers.")
                 headers_needed = True # Need to write headers manually

            # If template failed or wasn't used, create file and mark headers as needed
            if not file_exists:
                 # Create empty file if it wasn't created by template copy
                 # Ensure directory exists before creating file
                 if dir_path:
                     os.makedirs(dir_path, exist_ok=True)
                 open(self.filepath, 'a').close()
                 headers_needed = True

        # Check if existing file is empty (needs headers)
        # Add check for file_exists before getsize
        if file_exists and os.path.getsize(self.filepath) == 0:
            logger.info(f"Existing mapping file '{self.filepath}' is empty. Headers will be written.")
            headers_needed = True
        # Optional: Could add a check here to read the first line and verify headers if file is not empty

        # Write headers if needed
        if headers_needed:
            try:
                # Ensure directory exists before writing headers
                dir_path = os.path.dirname(self.filepath)
                if dir_path:
                    os.makedirs(dir_path, exist_ok=True)
                # Use 'w' to overwrite if empty or just created
                with open(self.filepath, 'w', newline='', encoding='utf-8') as csvfile:
                     fieldnames = ['name', 'organization', 'privateID', 'contactEmails']
                     writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                     writer.writeheader()
                     logger.info(f"Wrote headers to '{self.filepath}'.")
            except Exception as e:
                 logger.error(f"Failed to write headers to '{self.filepath}': {e}")


    def _load_mapping(self):
        """Loads the existing private ID mapping. Assumes file and headers exist."""
        try:
            with open(self.filepath, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                # Ensure fieldnames are present before iterating
                if not reader.fieldnames:
                     logger.error(f"Could not read headers from '{self.filepath}'. File might be empty or corrupted.")
                     return

                expected_headers = ['name', 'organization', 'privateID'] # Check for essential headers
                if not all(h in reader.fieldnames for h in expected_headers):
                     logger.error(f"Missing expected headers in '{self.filepath}'. Found: {reader.fieldnames}")
                     return

                count = 0
                for row in reader:
                    # Add basic validation for row data if needed
                    if row.get('name') and row.get('organization') and row.get('privateID'):
                        key = (row['name'], row['organization'])
                        self.mapping[key] = {
                            'privateID': row['privateID'],
                            'contactEmails': row.get('contactEmails', '') # Handle potentially missing email column
                        }
                        self.existing_ids.add(row['privateID'])
                        count += 1
                    else:
                        logger.warning(f"Skipping incomplete row in '{self.filepath}': {row}")

            logger.info(f"Loaded {count} existing entries from {self.filepath}")
        except FileNotFoundError:
             # This shouldn't happen if _ensure_file_with_headers worked
             logger.error(f"Private ID mapping file unexpectedly not found at {self.filepath} during load.")
        except Exception as e:
            logger.error(f"Error loading private ID mapping {self.filepath}: {e}", exc_info=True)


    def _generate_unique_id(self):
        """Generates a unique 6-digit random number string."""
        while True:
            new_id = str(random.randint(100000, 999999))
            if new_id not in self.existing_ids:
                self.existing_ids.add(new_id) # Add to prevent reuse in this run
                return new_id

    def get_or_generate_id(self, repo_name, organization, contact_emails=""):
        """Gets existing ID or generates a new one, queueing new ones for saving."""
        key = (repo_name, organization)
        if key in self.mapping:
            logger.debug(f"Found existing privateID for '{repo_name}' in org '{organization}'.")
            return self.mapping[key]['privateID']

        logger.debug(f"Generating new privateID for '{repo_name}' in org '{organization}'.") # Changed from INFO
        new_id = self._generate_unique_id()
        new_entry_data = {
            'name': repo_name,
            'organization': organization,
            'privateID': new_id,
            'contactEmails': contact_emails
        }

        # Add to in-memory mapping for current run
        self.mapping[key] = {
            'privateID': new_id,
            'contactEmails': contact_emails
        }
        # Queue the new entry to be saved later
        self.new_mappings_to_save.append(new_entry_data)
        # Increment counter
        self.new_ids_generated_count += 1
        logger.debug(f"Queued new privateID {new_id} for '{repo_name}' for saving.")

        # Removed direct file writing from here

        return new_id

    # Method to save queued mappings (called from main script)
    def save_new_mappings(self):
        """Appends all newly generated mappings to the CSV file."""
        if not self.new_mappings_to_save:
             logger.info("No new private IDs generated to save.")
             return

        logger.info(f"Saving {len(self.new_mappings_to_save)} new private ID mappings to {self.filepath}...")
        try:
            # Open in append mode
            with open(self.filepath, 'a', newline='', encoding='utf-8') as csvfile:
                # Headers should already exist from __init__/_load_mapping
                fieldnames = ['name', 'organization', 'privateID', 'contactEmails']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

                # Write all queued rows
                writer.writerows(self.new_mappings_to_save)

                # Force flush just in case
                csvfile.flush()
                os.fsync(csvfile.fileno())

            logger.info(f"Successfully saved {len(self.new_mappings_to_save)} new mappings.")
            self.new_mappings_to_save = [] # Clear the queue after saving

        except Exception as e:
            logger.error(f"Error saving new private ID mappings to {self.filepath}: {e}", exc_info=True)
            # Consider how to handle failure - maybe retry? Keep items in queue?

    def get_new_id_count(self):
        """Returns the count of new IDs generated during this run."""
        return self.new_ids_generated_count
