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
from datetime import datetime, timezone
# from filelock import FileLock # Removed filelock import
import threading # Added for lock
import logging

logger = logging.getLogger(__name__)

# ANSI escape codes for coloring output (if not already defined globally)
ANSI_RED = "\x1b[31;1m"  # Bold Red
ANSI_RESET = "\x1b[0m"   # Reset to default color

EXEMPTION_LOGGER_SYSTEM_CONTEXT = "ExemptionLoggerSystem"

class ExemptionLogger:
    """Handles loading and logging repository exemptions to a CSV file."""

    # Changed 'repoID' to 'privateID' to match the field name in code.json.
    # This column will store the prefixed repo_id (e.g., github_12345).
    EXPECTED_HEADER = ['privateID', 'repositoryName', 'usageType', 'exemptionText', 'timestamp']

    def __init__(self, filepath="output/exempted_log.csv", template_path=None): # Made template optional
        """
        Initializes the ExemptionLogger.

        Args:
            filepath (str): Path to the exemption log CSV file.
            template_path (str, optional): Path to the template CSV file. Defaults to None.
        """
        self.log_file_path = filepath # Assign filepath to self.log_file_path
        self.template_path = template_path # Store template path (though not used in simplified header logic)
        # Removed lock file path definition
        # self.lock_file_path = f"{self.log_file_path}.lock"
        self.lock = threading.Lock() # Initialize the lock
        self.fieldnames = self.EXPECTED_HEADER # Use class attribute
        # Counter for new exemptions logged during this run
        self.new_exemptions_logged_count = 0
        # Set to store repo names already logged (used in log_exemption)
        # Changed to store privateID (prefixed repo_id) for more accurate duplicate checking
        self.logged_exemptions_by_private_id = set()
        # Ensure file exists and headers are correct before loading
        self._ensure_log_file_header() # Simplified version below
        # Load existing entries to populate self.exempted_repos
        self._load_log()

    def _ensure_log_file_header(self):
        """Simplified: Ensures the log file exists and writes header only if file does not exist."""
        try:
            # Ensure directory exists first
            os.makedirs(os.path.dirname(self.log_file_path), exist_ok=True)

            # Check if file exists *before* trying to open
            if not os.path.isfile(self.log_file_path):
                logger.debug(f"_ensure_log_file_header: File '{self.log_file_path}' does not exist. Writing header.", extra={'org_group': EXEMPTION_LOGGER_SYSTEM_CONTEXT})
                try:
                    # Open in 'w' mode ONLY to write the header if file is missing
                    with open(self.log_file_path, 'w', newline='', encoding='utf-8') as csvfile:
                        writer = csv.DictWriter(csvfile, fieldnames=self.fieldnames)
                        writer.writeheader()
                    logger.info(f"Initialized log file with header: {self.log_file_path}", extra={'org_group': EXEMPTION_LOGGER_SYSTEM_CONTEXT})
                except IOError as e:
                    logger.error(f"Error initializing log file {self.log_file_path}: {e}", extra={'org_group': EXEMPTION_LOGGER_SYSTEM_CONTEXT})
                    raise # Re-raise critical error
            # else: # If file exists, do nothing in this function during this debug step
            #    logger.debug(f"Log file {self.log_file_path} already exists. Header check/verification skipped.")

        except Exception as e:
            logger.error(f"Error checking or initializing log file {self.log_file_path}: {e}", extra={'org_group': EXEMPTION_LOGGER_SYSTEM_CONTEXT})
            raise # Re-raise critical error


    def _load_log(self):
        """Loads existing repo names from the log file to prevent duplicate logging."""
        try:
            # Ensure file exists before trying to read
            if not os.path.isfile(self.log_file_path) or os.path.getsize(self.log_file_path) == 0:
                 logger.info(f"Exemption log file '{self.log_file_path}' is empty or non-existent. No existing entries to load.", extra={'org_group': EXEMPTION_LOGGER_SYSTEM_CONTEXT})
                 return

            with open(self.log_file_path, 'r', newline='', encoding='utf-8') as csvfile:
                # Use DictReader for easier access, check headers first
                # Peek at the first line to check header before creating DictReader
                first_line = csvfile.readline()
                if not first_line:
                    logger.warning(f"Exemption log file '{self.log_file_path}' appears empty after opening.", extra={'org_group': EXEMPTION_LOGGER_SYSTEM_CONTEXT})
                    return
                # Ensure comparison handles potential BOM or extra whitespace
                actual_header = [h.strip() for h in first_line.strip().split(',')]

                if actual_header != self.EXPECTED_HEADER:
                     logger.error(f"Header mismatch loading log file '{self.log_file_path}'. Expected: {self.EXPECTED_HEADER}, Found: {actual_header}. Cannot load entries.", extra={'org_group': EXEMPTION_LOGGER_SYSTEM_CONTEXT})
                     return

                # Reset file pointer and create DictReader
                csvfile.seek(0)
                reader = csv.DictReader(csvfile)
                # Fieldnames are now confirmed correct by the check above

                count = 0
                for row_num, row in enumerate(reader, start=2): # Start count from 2 (after header)
                    private_id_from_csv = row.get('privateID') # Use 'privateID' column
                    if private_id_from_csv:
                        # Add privateID to the set for quick lookup later
                        self.logged_exemptions_by_private_id.add(private_id_from_csv)
                        count += 1
                    else:
                         logger.warning(f"{ANSI_RED}Skipping row {row_num} with missing privateID in '{self.log_file_path}': {row}{ANSI_RESET}", extra={'org_group': EXEMPTION_LOGGER_SYSTEM_CONTEXT})
            logger.info(f"Loaded {count} existing exemption entries (repo names) from {self.log_file_path}", extra={'org_group': EXEMPTION_LOGGER_SYSTEM_CONTEXT})
        except FileNotFoundError:
            # Should be handled by _ensure_log_file_header, but good safety check
            logger.error(f"{ANSI_RED}Exemption log file unexpectedly not found at {self.log_file_path} during load.{ANSI_RESET}", extra={'org_group': EXEMPTION_LOGGER_SYSTEM_CONTEXT})
        except Exception as e:
            logger.error(f"{ANSI_RED}Error loading exemption log{ANSI_RESET} {self.log_file_path}: {e}", exc_info=True, extra={'org_group': EXEMPTION_LOGGER_SYSTEM_CONTEXT})

    def log_exemption(self, private_id_value: str, repo_name: str, usage_type: str, exemption_text: str):
        """Logs an exemption entry to the CSV file if not already logged."""
        org_group_context_for_log = private_id_value # private_id_value often contains org/repo
        # Check if already logged in this session or loaded from file
        if private_id_value in self.logged_exemptions_by_private_id:
            logger.debug(f"Exemption for privateID '{private_id_value}' (Repo: '{repo_name}') already logged. Skipping.", extra={'org_group': org_group_context_for_log})
            return False # Indicate not logged this time

        log_entry = {
            'privateID': private_id_value or '', # Store the prefixed repo_id under the 'privateID' key
            'repositoryName': repo_name,
            'usageType': usage_type,
            'exemptionText': exemption_text,
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        # lock = FileLock(self.lock_file_path) # Removed lock instantiation
        
        with self.lock: # Acquire lock before file operations
            try:
                logger.debug(f"log_exemption: Attempting to log for '{repo_name}'. Checking file '{self.log_file_path}' before open.", extra={'org_group': org_group_context_for_log})
                # Log first line before appending
                # This debug check might be less useful with locking, but kept for now
                if os.path.exists(self.log_file_path):
                     with open(self.log_file_path, 'r', encoding='utf-8') as check_file:
                         logger.debug(f"log_exemption: First line before append for '{repo_name}': '{check_file.readline().strip()}'", extra={'org_group': org_group_context_for_log})

                with open(self.log_file_path, 'a', newline='', encoding='utf-8') as csvfile:
                    is_empty = csvfile.tell() == 0
                    writer = csv.DictWriter(csvfile, fieldnames=self.fieldnames)
                    
                    if is_empty: # This check might be redundant if _ensure_log_file_header always creates it
                        writer.writeheader()
                    writer.writerow(log_entry)

                self.logged_exemptions_by_private_id.add(private_id_value)
                self.new_exemptions_logged_count += 1
                logger.debug(f"Logged exemption for '{repo_name}'", extra={'org_group': org_group_context_for_log})
                return True
            except IOError as e:
                logger.error(f"Error writing to log file {self.log_file_path}: {e}", extra={'org_group': org_group_context_for_log})
                return False
            except Exception as e:
                logger.error(f"Unexpected error logging exemption for {repo_name}: {e}", exc_info=True, extra={'org_group': org_group_context_for_log})
                return False
        # Lock is released automatically when exiting 'with self.lock:' block


    def get_new_exemption_count(self):
        """Returns the count of new exemptions logged during this run."""
        return self.new_exemptions_logged_count

    def save_all_exemptions(self):
        """
        Ensures all logged exemptions are persisted.
        In the current implementation, logging happens immediately, so this method
        primarily serves as a confirmation or for future batching capabilities.
        """
        logger.info(f"All {self.new_exemptions_logged_count} new exemptions were logged to {self.log_file_path}.", extra={'org_group': EXEMPTION_LOGGER_SYSTEM_CONTEXT})
 
# Example usage (if needed for testing, otherwise remove)
# if __name__ == '__main__':
#     logging.basicConfig(level=logging.DEBUG)
#     logger_instance = ExemptionLogger(filepath="output/test_exempted_log.csv")
#     logger_instance.log_exemption("TESTID1", "test-repo-1", "exemptTypeA", "Reason A")
#     logger_instance.log_exemption("TESTID2", "test-repo-2", "exemptTypeB", "Reason B")
#     logger_instance.log_exemption("TESTID3", "test-repo-1", "exemptTypeC", "Reason C") # Should skip
#     print(f"New exemptions logged: {logger_instance.get_new_exemption_count()}") # Should be 2
