# utils/config.py
"""
Configuration class for the Share IT Act Repository Scanning Tool.
Loads settings from environment variables.
"""
import os
import logging
from dotenv import load_dotenv

class Config:
    """
    Configuration class to hold and validate configuration settings.
    """
    def __init__(self):
        load_dotenv() # Load .env for non-auth configurations
        limit_str = os.getenv("LimitNumberOfRepos", "0").strip()
        try:
            self.DEBUG_REPO_LIMIT = int(limit_str)
            if self.DEBUG_REPO_LIMIT <= 0:
                self.DEBUG_REPO_LIMIT = None
        except ValueError:
            logging.getLogger(__name__).warning(f"LimitNumberOfRepos: '{limit_str}'. Defaulting to no limit.")
            self.DEBUG_REPO_LIMIT = None

        self.OUTPUT_DIR = os.getenv("OutputDir", "output").strip()
        self.CATALOG_JSON_FILE = os.getenv("catalogJsonFile", "code.json")
        self.EXEMPTION_LOG_FILENAME = os.getenv("ExemptedCSVFile", "exempted_log.csv")
        self.AGENCY_NAME = os.getenv("AGENCY_NAME", "CDC")
        self.PRIVATE_ID_FILENAME = os.getenv("PrivateIDCSVFile", "privateid_mapping.csv") 
        
        self.EXEMPTION_LOG_FILEPATH = os.path.join(self.OUTPUT_DIR, self.EXEMPTION_LOG_FILENAME)
        self.PRIVATE_ID_FILEPATH = os.path.join(self.OUTPUT_DIR, self.PRIVATE_ID_FILENAME)
        self.REPOS_CREATED_AFTER_DATE = os.getenv("REPOS_CREATED_AFTER_DATE", "")

        # --- AI Specific Configurations ---
        self.AI_ENABLED_ENV = os.getenv("AI_ENABLED", "False").lower() == "true"
        self.AI_MODEL_NAME_ENV = os.getenv("AI_MODEL_NAME", "gemini-1.0-pro-latest") # Default model
        self.AI_MAX_OUTPUT_TOKENS_ENV = int(os.getenv("AI_MAX_OUTPUT_TOKENS", "2048")) # Default max tokens
        self.MAX_TOKENS_ENV = int(os.getenv("MAX_TOKENS", "15000")) # For AI input truncation
        self.AI_TEMPERATURE_ENV = float(os.getenv("AI_TEMPERATURE", "0.4")) # Default AI temperature

        # --- Adaptive Delay Settings ---
        self.ADAPTIVE_DELAY_ENABLED_ENV = os.getenv("ADAPTIVE_DELAY_ENABLED", "false").lower() == "true"
        self.ADAPTIVE_DELAY_BASE_SECONDS_ENV = float(os.getenv("ADAPTIVE_DELAY_BASE_SECONDS", "0.1"))
        self.ADAPTIVE_DELAY_THRESHOLD_REPOS_ENV = int(os.getenv("ADAPTIVE_DELAY_THRESHOLD_REPOS", "50"))
        self.ADAPTIVE_DELAY_MAX_SECONDS_ENV = float(os.getenv("ADAPTIVE_DELAY_MAX_SECONDS", "2.0"))
        self.ADAPTIVE_DELAY_CACHE_MODIFIED_FACTOR_ENV = float(os.getenv("ADAPTIVE_DELAY_CACHE_MODIFIED_FACTOR", "0.10"))


        # --- GitHub Specific API Call Throttling ---
        self.GITHUB_API_CALL_DELAY_SECONDS_ENV = float(os.getenv("GITHUB_API_CALL_DELAY_SECONDS", "0.0"))

        self.INSTRUCTIONS_URL = os.getenv("INSTRUCTIONS_PDF_URL")
        self.EXEMPTED_NOTICE_URL = os.getenv("EXEMPTED_NOTICE_PDF_URL")
        self.PRIVATE_REPO_CONTACT_EMAIL = os.getenv("PRIVATE_REPO_CONTACT_EMAIL", "shareit@cdc.gov")

        # Platform-specific target lists from .env (used if not overridden by CLI)
        self.GITHUB_ORGS_ENV = [org.strip() for org in os.getenv("GITHUB_ORGS", "").split(',') if org.strip()]
        self.GITLAB_URL_ENV = os.getenv("GITLAB_URL", "https://gitlab.com")
        self.GITLAB_GROUPS_ENV = [group.strip() for group in os.getenv("GITLAB_GROUPS", "").split(',') if group.strip()]
        
        self.AZURE_DEVOPS_ORG_ENV = os.getenv("AZURE_DEVOPS_ORG")
        self.AZURE_DEVOPS_API_URL_ENV = os.getenv("AZURE_DEVOPS_API_URL", "https://dev.azure.com")
        self.AZURE_DEVOPS_TARGETS_RAW_ENV = [t.strip() for t in os.getenv("AZURE_DEVOPS_TARGETS", "").split(',') if t.strip()]

        hours_per_commit_str = os.getenv("HOURS_PER_COMMIT")
        if hours_per_commit_str is not None:
            try:
                self.HOURS_PER_COMMIT_ENV = float(hours_per_commit_str)
            except ValueError:
                logging.getLogger(__name__).warning(
                    f"Invalid value for HOURS_PER_COMMIT environment variable: '{hours_per_commit_str}'. "
                    "This setting will be ignored unless overridden by CLI."
                )
                self.HOURS_PER_COMMIT_ENV = None
        else:
            self.HOURS_PER_COMMIT_ENV = None
        
        try:
            self.SCANNER_MAX_WORKERS_ENV = int(os.getenv("SCANNER_MAX_WORKERS", "5")) # Default to 5 workers
            if self.SCANNER_MAX_WORKERS_ENV <= 0: # Ensure it's a positive number
                self.SCANNER_MAX_WORKERS_ENV = 5 
        except ValueError:
            logging.getLogger(__name__).warning(f"Invalid SCANNER_MAX_WORKERS value in .env. Defaulting to 5.")
            self.SCANNER_MAX_WORKERS_ENV = 5