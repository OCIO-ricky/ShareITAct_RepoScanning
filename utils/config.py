# utils/config.py
"""
Configuration class for the Share IT Act Repository Scanning Tool.
Loads settings from environment variables.
"""
import os
import logging
from dotenv import load_dotenv
from typing import Optional # Import Optional

logger = logging.getLogger(__name__)

class Config:
    """
    Configuration class to hold and validate configuration settings.
    """
    def __init__(self):
        load_dotenv() # Load .env for non-auth configurations

        # --- General Settings ---
        limit_str = os.getenv("LimitNumberOfRepos", "0").strip()
        try:
            self.DEBUG_REPO_LIMIT = int(limit_str)
            if self.DEBUG_REPO_LIMIT <= 0:
                self.DEBUG_REPO_LIMIT = None
        except ValueError:
            logger.warning(f"Invalid LimitNumberOfRepos: '{limit_str}'. Defaulting to no limit.")
            self.DEBUG_REPO_LIMIT = None

        self.OUTPUT_DIR = os.getenv("OutputDir", "output").strip()
        self.CATALOG_JSON_FILE = os.getenv("catalogJsonFile", "code.json")
        self.EXEMPTION_LOG_FILENAME = os.getenv("ExemptedCSVFile", "exempted_log.csv")
        self.AGENCY_NAME = os.getenv("AGENCY_NAME", "CDC")
        self.PRIVATE_ID_FILENAME = os.getenv("PrivateIDCSVFile", "privateid_mapping.csv")
        
        self.EXEMPTION_LOG_FILEPATH = os.path.join(self.OUTPUT_DIR, self.EXEMPTION_LOG_FILENAME)
        self.PRIVATE_ID_FILEPATH = os.path.join(self.OUTPUT_DIR, self.PRIVATE_ID_FILENAME)
        self.REPOS_CREATED_AFTER_DATE = os.getenv("REPOS_CREATED_AFTER_DATE", "") # Parsed in connectors/main script

        self.INSTRUCTIONS_URL = os.getenv("INSTRUCTIONS_PDF_URL")
        self.EXEMPTED_NOTICE_URL = os.getenv("EXEMPTED_NOTICE_PDF_URL")
        self.PRIVATE_REPO_CONTACT_EMAIL = os.getenv("PRIVATE_REPO_CONTACT_EMAIL", "shareit@cdc.gov")
        self.DEFAULT_CONTACT_EMAIL = os.getenv("DEFAULT_CONTACT_EMAIL", "shareit@cdc.gov") # For public repos

        # --- AI Specific Configurations ---
        self.AI_ENABLED_ENV = os.getenv("AI_ENABLED", "False").lower() == "true"
        self.AI_MODEL_NAME_ENV = os.getenv("AI_MODEL_NAME", "gemini-1.0-pro-latest")
        self.AI_MAX_OUTPUT_TOKENS_ENV = int(os.getenv("AI_MAX_OUTPUT_TOKENS", "2048"))
        self.MAX_TOKENS_ENV = int(os.getenv("MAX_TOKENS", "15000")) # For AI input truncation
        self.AI_TEMPERATURE_ENV = float(os.getenv("AI_TEMPERATURE", "0.2")) # Adjusted to match .env
        self.AI_AUTO_DISABLED_SSL_ERROR = False # Initialize the new attribute
        self.AI_ORGANIZATION_ENABLED_ENV = os.getenv("AI_ORGANIZATION_ENABLED", "False").lower() == "true"
        self.AI_DELAY_ENABLED_ENV = float(os.getenv("AI_DELAY_ENABLED", "0.0"))

        # --- Simplified Rate Limiting Configuration ---
        self.API_SAFETY_FACTOR_ENV = float(os.getenv("API_SAFETY_FACTOR", "0.8")) # Use 80% of available quota
        self.MIN_INTER_REPO_DELAY_SECONDS_ENV = float(os.getenv("MIN_INTER_REPO_DELAY_SECONDS", "0.1"))
        self.MAX_INTER_REPO_DELAY_SECONDS_ENV = float(os.getenv("MAX_INTER_REPO_DELAY_SECONDS", "30.0")) # Max delay between submitting repos
        self.ESTIMATED_LABOR_CALLS_PER_REPO_ENV = int(os.getenv("ESTIMATED_LABOR_CALLS_PER_REPO", "3")) # Rough estimate for labor hour calls
        # New settings for peek-ahead optimization
        self.PEEK_AHEAD_THRESHOLD_DELAY_SECONDS_ENV = float(os.getenv("PEEK_AHEAD_THRESHOLD_DELAY_SECONDS", "0.5")) # Only peek if standard delay is > this
        self.CACHE_HIT_SUBMISSION_DELAY_SECONDS_ENV = float(os.getenv("CACHE_HIT_SUBMISSION_DELAY_SECONDS", "0.05")) # Delay for likely cache hits

        self.FIXED_PRIVATE_REPO_FILTER_DATE_ENV = os.getenv("FIXED_PRIVATE_REPO_FILTER_DATE", "2021-04-21") # Default fixed date

        # --- Platform-specific target lists from .env (used if not overridden by CLI) ---
        self.GITHUB_ORGS_ENV = [org.strip() for org in os.getenv("GITHUB_ORGS", "").split(',') if org.strip()]
        
        self.GITLAB_URL_ENV = os.getenv("GITLAB_URL", "https://gitlab.com")
        self.GITLAB_GROUPS_ENV = [group.strip() for group in os.getenv("GITLAB_GROUPS", "").split(',') if group.strip()]
        
        self.AZURE_DEVOPS_ORG_ENV = os.getenv("AZURE_DEVOPS_ORG") # Default Org for Azure if project name only is given
        self.AZURE_DEVOPS_API_URL_ENV = os.getenv("AZURE_DEVOPS_API_URL", "https://dev.azure.com")
        self.AZURE_DEVOPS_TARGETS_RAW_ENV = [t.strip() for t in os.getenv("AZURE_DEVOPS_TARGETS", "").split(',') if t.strip()]

        # --- Labor Hours Estimation ---
        hours_per_commit_str = os.getenv("HOURS_PER_COMMIT")
        if hours_per_commit_str is not None:
            try:
                self.HOURS_PER_COMMIT_ENV = float(hours_per_commit_str)
            except ValueError:
                logger.warning(
                    f"Invalid value for HOURS_PER_COMMIT environment variable: '{hours_per_commit_str}'. "
                    "This setting will be ignored unless overridden by CLI."
                )
                self.HOURS_PER_COMMIT_ENV = None
        else:
            self.HOURS_PER_COMMIT_ENV = None
        
        # --- Scanner Concurrency ---
        try:
            self.SCANNER_MAX_WORKERS_ENV = int(os.getenv("SCANNER_MAX_WORKERS", "5")) # Default to 5 workers
            if self.SCANNER_MAX_WORKERS_ENV <= 0: # Ensure it's a positive number
                logger.warning(f"SCANNER_MAX_WORKERS must be positive. Defaulting to 5.")
                self.SCANNER_MAX_WORKERS_ENV = 5
        except ValueError:
            logger.warning(f"Invalid SCANNER_MAX_WORKERS value in .env. Defaulting to 5.")
            self.SCANNER_MAX_WORKERS_ENV = 5

        # --- Platform-wide API call estimates (populated by orchestrator) ---
        self.GITHUB_TOTAL_ESTIMATED_API_CALLS: Optional[int] = None
        self.GITLAB_TOTAL_ESTIMATED_API_CALLS: Optional[int] = None
        self.AZURE_TOTAL_ESTIMATED_API_CALLS: Optional[int] = None

        # --- Dynamically load any other environment variables as attributes ---
        # This allows flexibility for less critical or temporary settings without
        # needing to explicitly define them in the class.
        # Explicitly defined attributes above will take precedence if names collide.
        for key, value in os.environ.items():
            attr_name = f"{key}_ENV"
            if not hasattr(self, attr_name): # Only set if not already explicitly defined
                setattr(self, attr_name, value)
            elif getattr(self, attr_name) is None and value is not None:
                # If explicitly defined as None (e.g. from a failed float conversion)
                # but an env var exists, prefer the env var string.
                # This case is less likely with current explicit loading logic.
                setattr(self, attr_name, value)