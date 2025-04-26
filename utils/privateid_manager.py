# utils/privateid_manager.py
import csv
import os
import random
import logging

logger = logging.getLogger(__name__)

class PrivateIdManager:
    """Handles loading, generating, and saving private IDs and contact emails."""

    # --- Define fieldnames including contactEmails ---
    FIELDNAMES = ['name', 'organization', 'privateID', 'contactEmails']

    def __init__(self, filepath, template_path="templates/privateid_mapping_template.csv"):
        """Initializes the PrivateIdManager."""
        self.filepath = filepath
        self.template_path = template_path
        self.mapping = {}
        self.existing_ids = set()
        self.new_ids_generated_count = 0
        self.new_mappings_to_save = []
        self._ensure_file_with_headers()
        self._load_mapping()

    def _ensure_file_with_headers(self):
        """Ensures the mapping file exists and has the correct headers."""
        file_exists = os.path.isfile(self.filepath)
        headers_needed = False
        write_mode = 'w' # Default to write if creating or empty

        if not file_exists:
            dir_path = os.path.dirname(self.filepath)
            if dir_path:
                 os.makedirs(dir_path, exist_ok=True)
            logger.info(f"Mapping file not found. Creating '{self.filepath}'.")
            try:
                with open(self.template_path, 'r', encoding='utf-8') as tpl, open(self.filepath, 'w', encoding='utf-8') as out:
                    template_content = tpl.read()
                    if template_content.strip():
                        out.write(template_content)
                        logger.info(f"Created private ID mapping from template: {self.filepath}")
                        file_exists = True
                        # Check if template headers match FIELDNAMES
                        with open(self.filepath, 'r', encoding='utf-8') as check_file:
                            reader = csv.reader(check_file)
                            try:
                                headers = next(reader)
                                if headers != self.FIELDNAMES:
                                    logger.warning(f"Template headers mismatch expected headers. Will overwrite. Template: {headers}, Expected: {self.FIELDNAMES}")
                                    headers_needed = True # Force rewrite
                                else:
                                     write_mode = 'a' # Template is good, append later
                            except StopIteration: # Empty template
                                headers_needed = True
                    else:
                        logger.warning(f"Template file '{self.template_path}' is empty. Will write headers directly.")
                        headers_needed = True
            except FileNotFoundError:
                logger.warning(f"Template file not found at {self.template_path}. Will create mapping file with headers.")
                headers_needed = True
            except Exception as e:
                 logger.error(f"Error copying template file '{self.template_path}': {e}. Will create mapping file with headers.")
                 headers_needed = True

            if not file_exists:
                 if dir_path:
                     os.makedirs(dir_path, exist_ok=True)
                 open(self.filepath, 'a').close()
                 headers_needed = True

        if file_exists and os.path.getsize(self.filepath) == 0:
            logger.info(f"Existing mapping file '{self.filepath}' is empty. Headers will be written.")
            headers_needed = True
            write_mode = 'w'
        elif file_exists and not headers_needed: # Check headers if file exists and wasn't created/template checked
             try:
                 with open(self.filepath, 'r', newline='', encoding='utf-8') as csvfile:
                     reader = csv.DictReader(csvfile)
                     if not reader.fieldnames or reader.fieldnames != self.FIELDNAMES:
                         logger.warning(f"Headers in existing file '{self.filepath}' are missing or incorrect. Will overwrite. Found: {reader.fieldnames}, Expected: {self.FIELDNAMES}")
                         headers_needed = True
                         write_mode = 'w'
                     else:
                         write_mode = 'a' # Headers are good, append later
             except Exception as e:
                  logger.error(f"Error checking headers in '{self.filepath}': {e}. Assuming headers need writing.")
                  headers_needed = True
                  write_mode = 'w'

        if headers_needed:
            try:
                dir_path = os.path.dirname(self.filepath)
                if dir_path:
                    os.makedirs(dir_path, exist_ok=True)
                with open(self.filepath, write_mode, newline='', encoding='utf-8') as csvfile:
                     writer = csv.DictWriter(csvfile, fieldnames=self.FIELDNAMES)
                     writer.writeheader()
                     logger.info(f"Wrote headers {self.FIELDNAMES} to '{self.filepath}'.")
            except Exception as e:
                 logger.error(f"Failed to write headers to '{self.filepath}': {e}")

    def _load_mapping(self):
        """Loads the existing private ID mapping including contact emails."""
        try:
            with open(self.filepath, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                if not reader.fieldnames:
                     logger.error(f"Could not read headers from '{self.filepath}'. File might be empty or corrupted.")
                     return

                # Check for essential headers
                if not all(h in reader.fieldnames for h in ['name', 'organization', 'privateID']):
                     logger.error(f"Missing essential headers in '{self.filepath}'. Found: {reader.fieldnames}")
                     return

                count = 0
                for row in reader:
                    if row.get('name') and row.get('organization') and row.get('privateID'):
                        key = (row['name'], row['organization'])
                        self.mapping[key] = {
                            'privateID': row['privateID'],
                            # Load contactEmails, default to empty string if column missing/empty
                            'contactEmails': row.get('contactEmails', '')
                        }
                        self.existing_ids.add(row['privateID'])
                        count += 1
                    else:
                        logger.warning(f"Skipping incomplete row in '{self.filepath}': {row}")

            logger.info(f"Loaded {count} existing entries from {self.filepath}")
        except FileNotFoundError:
             logger.error(f"Private ID mapping file unexpectedly not found at {self.filepath} during load.")
        except Exception as e:
            logger.error(f"Error loading private ID mapping {self.filepath}: {e}", exc_info=True)

    def _generate_unique_id(self):
        """Generates a unique 6-digit random number string."""
        while True:
            new_id = str(random.randint(100000, 999999))
            if new_id not in self.existing_ids:
                self.existing_ids.add(new_id)
                return new_id

    # --- Updated to accept contact_emails ---
    def get_or_generate_id(self, repo_name, organization, contact_emails=""):
        """Gets existing ID or generates a new one including contact emails."""
        key = (repo_name, organization)
        if key in self.mapping:
            logger.debug(f"Found existing privateID for '{repo_name}' in org '{organization}'.")
            # Optional: Update contact emails if provided and different? For now, just return existing ID.
            # if contact_emails and self.mapping[key].get('contactEmails') != contact_emails:
            #     logger.info(f"Contact emails for existing entry '{repo_name}' differ. Keeping existing ID, not updating emails here.")
            return self.mapping[key]['privateID']

        logger.debug(f"Generating new privateID for '{repo_name}' in org '{organization}'.")
        new_id = self._generate_unique_id()
        new_entry_data = {
            'name': repo_name,
            'organization': organization,
            'privateID': new_id,
            'contactEmails': contact_emails # Store provided emails
        }

        self.mapping[key] = {
            'privateID': new_id,
            'contactEmails': contact_emails
        }
        self.new_mappings_to_save.append(new_entry_data)
        self.new_ids_generated_count += 1
        logger.debug(f"Queued new privateID {new_id} with emails '{contact_emails}' for '{repo_name}' for saving.")

        return new_id

    def save_new_mappings(self):
        """Appends all newly generated mappings (including emails) to the CSV file."""
        if not self.new_mappings_to_save:
             logger.info("No new private IDs generated to save.")
             return

        logger.info(f"Saving {len(self.new_mappings_to_save)} new private ID mappings to {self.filepath}...")
        try:
            with open(self.filepath, 'a', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=self.FIELDNAMES)
                # Check if file is empty AFTER opening in append mode, write header if needed
                # This is a fallback in case _ensure_file_with_headers failed somehow
                csvfile.seek(0, os.SEEK_END) # Go to end of file
                if csvfile.tell() == 0: # Check if file is empty
                    writer.writeheader()
                    logger.warning(f"File '{self.filepath}' was empty before saving new mappings. Wrote headers.")

                writer.writerows(self.new_mappings_to_save)
                csvfile.flush()
                os.fsync(csvfile.fileno())

            logger.info(f"Successfully saved {len(self.new_mappings_to_save)} new mappings.")
            self.new_mappings_to_save = []

        except Exception as e:
            logger.error(f"Error saving new private ID mappings to {self.filepath}: {e}", exc_info=True)

    def get_new_id_count(self):
        """Returns the count of new IDs generated during this run."""
        return self.new_ids_generated_count

