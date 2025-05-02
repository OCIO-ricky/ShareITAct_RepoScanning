# utils/exemption_logger.py
"""
Manages the logging of repository exemptions to a dedicated CSV file.

This module provides the `ExemptionLogger` class, which handles the creation,
loading, and appending of exemption records. Its primary goal is to maintain
a persistent log of repositories identified as exempt, along with the reason
and timestamp.

"""
import csv
import os
from datetime import datetime, timezone # Added timezone
import logging

logger = logging.getLogger(__name__)

class ExemptionLogger:
    """Handles loading and logging repository exemptions to a CSV file."""

    EXPECTED_HEADER = ['privateID', 'repositoryName', 'usageType', 'exemptionText', 'timestamp']

    def __init__(self, filepath="output/exempted_log.csv", template_path=None): # Made template optional
        """
        Initializes the ExemptionLogger.

        Args:
            filepath (str): Path to the exemption log CSV file.
            template_path (str, optional): Path to the template CSV file. Defaults to None.
        """
        self.filepath = filepath
        self.template_path = template_path # Store template path if provided
        # Set to store repo_names that are already exempted (loaded or logged this run)
        self.exempted_repos = set()
        # Counter for new exemptions logged during this run
        self.new_exemptions_logged_count = 0
        self._ensure_file_with_headers()
        self._load_log()

    def _ensure_file_with_headers(self):
        """Ensures the exemption log file exists and has the correct headers."""
        file_exists = os.path.isfile(self.filepath)
        headers_needed = False
        write_mode = 'a' # Default to append

        if not file_exists:
            os.makedirs(os.path.dirname(self.filepath), exist_ok=True)
            logger.info(f"Exemption log file not found. Creating '{self.filepath}'.")
            headers_needed = True
            write_mode = 'w' # Write mode to create and add header
            # Attempt to copy from template if provided
            if self.template_path and os.path.exists(self.template_path):
                 try:
                      with open(self.template_path, 'r', encoding='utf-8') as tpl, open(self.filepath, 'w', encoding='utf-8') as out:
                           template_content = tpl.read()
                           # Check if template header matches expected header
                           first_line = template_content.splitlines()[0] if template_content else ""
                           if first_line.split(',') == self.EXPECTED_HEADER:
                                out.write(template_content)
                                logger.info(f"Created exemption log from template: {self.filepath}")
                                headers_needed = False # Header copied from template
                           else:
                                logger.warning(f"Template header in '{self.template_path}' does not match expected. Writing correct headers.")
                 except Exception as e:
                      logger.error(f"Error copying template file '{self.template_path}': {e}. Will create file with correct headers.")
            else:
                 if self.template_path:
                      logger.warning(f"Template file not found at {self.template_path}. Creating file with correct headers.")

        elif os.path.getsize(self.filepath) == 0:
            logger.info(f"Existing exemption log file '{self.filepath}' is empty. Headers will be written.")
            headers_needed = True
            write_mode = 'w' # Overwrite empty file with header
        else:
            # Check header of existing, non-empty file
            try:
                with open(self.filepath, 'r', newline='', encoding='utf-8') as csvfile:
                    reader = csv.reader(csvfile)
                    header = next(reader)
                    if header != self.EXPECTED_HEADER:
                        logger.error(f"Header mismatch in existing log file '{self.filepath}'. Expected: {self.EXPECTED_HEADER}, Found: {header}. Please fix the file manually.")
                        # Consider raising an error or exiting if header mismatch is critical
                        # raise ValueError(f"Header mismatch in {self.filepath}")
            except StopIteration: # File exists but is somehow empty after size check?
                 logger.warning(f"Could not read header from non-empty file '{self.filepath}'. Assuming headers needed.")
                 headers_needed = True
                 write_mode = 'w'
            except Exception as e:
                 logger.error(f"Error reading header from existing file '{self.filepath}': {e}. Cannot proceed safely.")
                 raise # Re-raise the exception

        # Write headers if needed (using the determined write_mode)
        if headers_needed:
            try:
                with open(self.filepath, write_mode, newline='', encoding='utf-8') as csvfile:
                     writer = csv.writer(csvfile) # Use standard writer
                     writer.writerow(self.EXPECTED_HEADER)
                     logger.info(f"Wrote headers to '{self.filepath}'.")
            except Exception as e:
                 logger.error(f"Failed to write headers to '{self.filepath}': {e}")
                 raise # Re-raise the exception

    def _load_log(self):
        """Loads existing exemptions from the log file."""
        try:
            # Ensure file exists before trying to read
            if not os.path.isfile(self.filepath) or os.path.getsize(self.filepath) == 0:
                 logger.info(f"Exemption log file '{self.filepath}' is empty or non-existent. No existing entries to load.")
                 return

            with open(self.filepath, 'r', newline='', encoding='utf-8') as csvfile:
                # Use DictReader for easier access, check headers first
                # Peek at the first line to check header before creating DictReader
                first_line = csvfile.readline()
                if not first_line:
                    logger.warning(f"Exemption log file '{self.filepath}' appears empty after opening.")
                    return
                actual_header = [h.strip() for h in first_line.strip().split(',')]

                if actual_header != self.EXPECTED_HEADER:
                     logger.error(f"Header mismatch loading log file '{self.filepath}'. Expected: {self.EXPECTED_HEADER}, Found: {actual_header}. Cannot load entries.")
                     return

                # Reset file pointer and create DictReader
                csvfile.seek(0)
                reader = csv.DictReader(csvfile)
                # Fieldnames are now confirmed correct by the check above

                count = 0
                for row_num, row in enumerate(reader, start=2): # Start count from 2 (after header)
                    repo_name = row.get('repositoryName')
                    if repo_name:
                        self.exempted_repos.add(repo_name)
                        count += 1
                    else:
                         logger.warning(f"Skipping row {row_num} with missing repositoryName in '{self.filepath}': {row}")
            logger.info(f"Loaded {count} existing exemption entries from {self.filepath}")
        except FileNotFoundError:
            # Should be handled by _ensure_file_with_headers, but good safety check
            logger.error(f"Exemption log file unexpectedly not found at {self.filepath} during load.")
        except Exception as e:
            logger.error(f"Error loading exemption log {self.filepath}: {e}", exc_info=True)

    # --- UPDATE Method Signature and writerow call: Remove 'reason' ---
    def log_exemption(self, private_id, repo_name, usage_type, exemption_text):
        """Logs an exemption if the repository hasn't been logged already."""
        if repo_name in self.exempted_repos:
            logger.debug(f"Repository '{repo_name}' already logged as exempted. Skipping.")
            return False # Indicate not logged this time

        timestamp = datetime.now(timezone.utc).isoformat() # Use timezone.utc
        log_entry = {
            'privateID': private_id or '', # Ensure it's not None
            'repositoryName': repo_name,
            # 'reason': reason, # Removed
            'usageType': usage_type,
            'exemptionText': exemption_text,
            'timestamp': timestamp
        }

        try:
            # Append the new entry - assumes file exists with headers
            with open(self.filepath, 'a', newline='', encoding='utf-8') as csvfile:
                # Use the expected header directly
                writer = csv.DictWriter(csvfile, fieldnames=self.EXPECTED_HEADER)
                # Write the dictionary row
                writer.writerow(log_entry)

            # Update in-memory set and counter only if write succeeds
            self.exempted_repos.add(repo_name)
            self.new_exemptions_logged_count += 1
            logger.debug(f"Logged exemption for '{repo_name}'") # Simplified log
            return True # Indicate logged successfully

        except Exception as e:
            logger.error(f"Error writing to exemption log {self.filepath}: {e}", exc_info=True)
            return False

    def get_new_exemption_count(self):
        """Returns the count of new exemptions logged during this run."""
        return self.new_exemptions_logged_count

