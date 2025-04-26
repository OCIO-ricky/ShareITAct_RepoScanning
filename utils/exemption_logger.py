# utils/exemption_logger.py
import csv
import os
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class ExemptionLogger:
    """Handles loading and logging repository exemptions to a CSV file."""

    def __init__(self, filepath, template_path="templates/exempted_log_template.csv"):
        """
        Initializes the ExemptionLogger.

        Args:
            filepath (str): Path to the exemption log CSV file.
            template_path (str): Path to the template CSV file.
        """
        self.filepath = filepath
        self.template_path = template_path
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

        if not file_exists:
            # Create directory if needed
            os.makedirs(os.path.dirname(self.filepath), exist_ok=True)
            logger.info(f"Exemption log file not found. Creating '{self.filepath}'.")
            # Try copying from template first
            try:
                with open(self.template_path, 'r', encoding='utf-8') as tpl, open(self.filepath, 'w', encoding='utf-8') as out:
                    template_content = tpl.read()
                    if template_content.strip():
                        out.write(template_content)
                        logger.info(f"Created exemption log from template: {self.filepath}")
                        file_exists = True # File now exists
                    else:
                        logger.warning(f"Template file '{self.template_path}' is empty. Will write headers directly.")
                        headers_needed = True
            except FileNotFoundError:
                logger.warning(f"Template file not found at {self.template_path}. Will create exemption log file with headers.")
                headers_needed = True
            except Exception as e:
                 logger.error(f"Error copying template file '{self.template_path}': {e}. Will create exemption log file with headers.")
                 headers_needed = True

            # If template failed or wasn't used, create file and mark headers as needed
            if not file_exists:
                 open(self.filepath, 'a').close() # Create empty file
                 headers_needed = True

        # Check if existing file is empty (needs headers)
        if file_exists and os.path.getsize(self.filepath) == 0:
            logger.info(f"Existing exemption log file '{self.filepath}' is empty. Headers will be written.")
            headers_needed = True

        # Write headers if needed
        if headers_needed:
            try:
                with open(self.filepath, 'w', newline='', encoding='utf-8') as csvfile: # Use 'w'
                     fieldnames = ['privateID', 'repositoryName', 'reason', 'usageType', 'exemptionText', 'timestamp']
                     writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                     writer.writeheader()
                     logger.info(f"Wrote headers to '{self.filepath}'.")
            except Exception as e:
                 logger.error(f"Failed to write headers to '{self.filepath}': {e}")

    def _load_log(self):
        """Loads existing exemptions from the log file."""
        try:
            with open(self.filepath, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                if not reader.fieldnames:
                     logger.error(f"Could not read headers from '{self.filepath}'. File might be empty or corrupted.")
                     return

                expected_headers = ['repositoryName'] # Only need repo name for duplicate check
                if not all(h in reader.fieldnames for h in expected_headers):
                     logger.error(f"Missing expected headers in '{self.filepath}'. Found: {reader.fieldnames}")
                     return

                count = 0
                for row in reader:
                    repo_name = row.get('repositoryName')
                    if repo_name:
                        self.exempted_repos.add(repo_name)
                        count += 1
                    else:
                         logger.warning(f"Skipping row with missing repositoryName in '{self.filepath}': {row}")
            logger.info(f"Loaded {count} existing exemption entries from {self.filepath}")
        except FileNotFoundError:
            # This shouldn't happen if _ensure_file_with_headers worked
            logger.error(f"Exemption log file unexpectedly not found at {self.filepath} during load.")
        except Exception as e:
            logger.error(f"Error loading exemption log {self.filepath}: {e}", exc_info=True)

    def log_exemption(self, private_id, repo_name, reason, usage_type, exemption_text):
        """Logs an exemption if the repository hasn't been logged already."""
        if repo_name in self.exempted_repos:
            logger.debug(f"Repository '{repo_name}' already logged as exempted. Skipping.")
            return False # Indicate not logged this time

        timestamp = datetime.now().isoformat()
        log_entry = {
            'privateID': private_id or '', # Ensure it's not None
            'repositoryName': repo_name,
            'reason': reason,
            'usageType': usage_type,
            'exemptionText': exemption_text,
            'timestamp': timestamp
        }

        try:
            # Append the new entry - assumes file exists with headers
            with open(self.filepath, 'a', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['privateID', 'repositoryName', 'reason', 'usageType', 'exemptionText', 'timestamp']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writerow(log_entry)
                # Optional: Add flush/fsync if experiencing write issues
                # csvfile.flush()
                # os.fsync(csvfile.fileno())

            # Update in-memory set and counter only if write succeeds
            self.exempted_repos.add(repo_name)
            self.new_exemptions_logged_count += 1
            logger.debug(f"Logged exemption for '{repo_name}': {reason}") # Changed to DEBUG
            return True # Indicate logged successfully

        except Exception as e:
            logger.error(f"Error writing to exemption log {self.filepath}: {e}", exc_info=True)
            return False

    def get_new_exemption_count(self):
        """Returns the count of new exemptions logged during this run."""
        return self.new_exemptions_logged_count
