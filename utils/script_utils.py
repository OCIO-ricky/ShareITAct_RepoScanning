# utils/script_utils.py
"""
Utility functions for the Share IT Act Repository Scanning Tool's main script.
Includes helpers for logging, file operations, data processing, and CLI arguments.
"""
import os
import json
import logging
import logging.handlers
import re
import sys
import shutil
from datetime import datetime, timezone, timedelta
from typing import List, Optional, Dict, Any

# Import necessary classes from other utility modules for type hinting
from .config import Config
from .exemption_logger import ExemptionLogger
from .privateid_manager import RepoIdMappingManager
from .logging_config import ContextualLogFormatter # Import ContextualLogFormatter

# --- Check for packaging library for version parsing ---
PACKAGING_AVAILABLE = False
try:
    import packaging.version as packaging_version
    PACKAGING_AVAILABLE = True
    logging.getLogger(__name__).info("Using 'packaging' library for version parsing.")
except ImportError:
    packaging_version = None # Define for type hinting if not available
    logging.getLogger(__name__).warning("Optional library 'packaging' not found. Version parsing will use basic regex (less reliable). Install with: pip install packaging")

# --- Constants moved from generate_codejson.py ---
INACTIVITY_THRESHOLD_YEARS = 2
VALID_README_STATUSES = {'maintained', 'deprecated', 'experimental', 'active', 'inactive'}
LOG_DIR_NAME = "logs"

# --- Logging Setup ---
class ContextualLogFormatter(logging.Formatter):
    def format(self, record):
        # Set a default for org_group if not present in the log record
        if not hasattr(record, 'org_group'):
            record.org_group = '------'  # Default 6-character value
        else:
            # Ensure org_group is at least 6 characters (pad if shorter, keep if longer)
            org_group_str = str(record.org_group)
            if len(org_group_str) < 6:
                record.org_group = org_group_str.ljust(6)  # Pad with spaces to minimum 6 chars
            # If 6 or more chars, leave as is
        
        return super().format(record)
    
def setup_global_logging(log_level=logging.INFO):
    """
    Set up global logging configuration.
    """
    log_directory = LOG_DIR_NAME # Use the constant
    log_file = os.path.join(log_directory, "generate_codejson_main.log")
    os.makedirs(log_directory, exist_ok=True)
    log_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    logger = logging.getLogger()
    if logger.hasHandlers():
        logger.handlers.clear()
    logger.setLevel(log_level)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(log_format)
    logger.addHandler(console_handler)

    try:
        file_handler = logging.handlers.RotatingFileHandler(
            log_file, maxBytes=5*1024*1024, backupCount=3, encoding='utf-8'
        )
        file_handler.setFormatter(log_format)
        logger.addHandler(file_handler)
        logging.info(f"Global logging configured. Main log: {log_file}")
    except Exception as e:
        logging.error(f"Failed to configure global file logging to {log_file}: {e}")
        logging.info("Global logging configured: Outputting to console only.")

def setup_target_logger(logger_name, log_file_name, output_dir, level=logging.INFO):
    """
    Sets up a logger with a rotating file handler.
    """
    log_dir = os.path.join(output_dir, LOG_DIR_NAME) # Use the constant
    os.makedirs(log_dir, exist_ok=True)
    log_file_path = os.path.join(log_dir, log_file_name)

    logger = logging.getLogger(logger_name)
    logger.setLevel(level)
    
    if logger.hasHandlers():
        logger.handlers.clear()
            
    logger.info(f"Logger {logger_name} set up with log file: {log_file_path}")

    fh = logging.FileHandler(log_file_path, mode='w', encoding='utf-8')
    fh.setLevel(level)
    # Use ContextualLogFormatter for target-specific logs
    formatter = ContextualLogFormatter('%(asctime)s - [%(org_group)s] - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    
    return logger

# --- File Operations ---
def write_json_file(data, filepath):
    try:
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        logging.getLogger(__name__).info(f"Successfully wrote data to {filepath}")
        return True
    except Exception as e:
        logging.getLogger(__name__).error(f"Error writing JSON to {filepath}: {e}", exc_info=True)
        return False

def backup_existing_file(output_dir, filename):
    """
    Backs up the existing file with a timestamped filename in the same directory.
    Note: The original file is gone after it is renamed.
    """
    logger_instance = logging.getLogger(__name__)
    current_filepath = os.path.join(output_dir, filename)
    if os.path.isfile(current_filepath):
        try:
            now = datetime.now()
            timestamp_str = now.strftime("%Y%m%d_%H%M%S")
            base_name, ext_dot = os.path.splitext(filename)
            ext = ext_dot if ext_dot else ""
            backup_filename = f"{base_name}_{timestamp_str}{ext}"
            backup_filepath = os.path.join(output_dir, backup_filename)
            counter = 1
            while os.path.exists(backup_filepath):
                 backup_filename = f"{base_name}_{timestamp_str}_{counter}{ext}"
                 backup_filepath = os.path.join(output_dir, backup_filename)
                 counter += 1
            os.rename(current_filepath, backup_filepath)
            logger_instance.info(f"Backed up existing '{filename}' to '{backup_filename}'.")
        except Exception as e:
            logger_instance.error(f"Error backing up '{filename}': {e}", exc_info=True)

def backup_file_and_leave_original(output_dir, filename):
    """
    Creates a backup copy of an existing file while leaving the original file intact.
    """
    logger_instance = logging.getLogger(__name__)
    current_filepath = os.path.join(output_dir, filename)
    if os.path.isfile(current_filepath):
        try:
            now = datetime.now()
            timestamp_str = now.strftime("%Y%m%d_%H%M%S")
            base_name, ext_dot = os.path.splitext(filename)
            ext = ext_dot if ext_dot else ""
            backup_filename = f"{base_name}_{timestamp_str}{ext}"
            backup_filepath = os.path.join(output_dir, backup_filename)
            counter = 1
            while os.path.exists(backup_filepath):
                 backup_filename = f"{base_name}_{timestamp_str}_{counter}{ext}"
                 backup_filepath = os.path.join(output_dir, backup_filename)
                 counter += 1
            
            shutil.copy2(current_filepath, backup_filepath)
            logger_instance.info(f"Backed up existing '{filename}' to '{backup_filename}' (original preserved).")
        except Exception as e:
            logger_instance.error(f"Error backing up '{filename}': {e}", exc_info=True)

# --- Data Processing and Inference ---
def parse_semver(tag_name: str) -> Any: # Return type can be packaging_version.Version or str
    """
    Parses a semantic version string and returns a Version object or string.
    """
    if not tag_name or not isinstance(tag_name, str): return None
    cleaned_tag = re.sub(r'^(v|release-|Release-|jenkins-\S+-)', '', tag_name.strip())
    if PACKAGING_AVAILABLE and packaging_version:
        try:
            return packaging_version.parse(cleaned_tag)
        except packaging_version.InvalidVersion:
            return None
    else: # Regex fallback
        if re.match(r'^\d+\.\d+(\.\d+)?($|[.-])', cleaned_tag):
             return cleaned_tag
        return None

def infer_version(repo_data: Dict[str, Any], logger_instance: logging.Logger) -> str:
    """
    Infers the version of the repository based on the tags.
    """
    repo_org_group_context = f"{repo_data.get('organization', 'UnknownOrg')}/{repo_data.get('name', 'UnknownRepo')}"
    api_tags = repo_data.get('_api_tags', []) 
    if not api_tags: return "N/A"
    parsed_versions, parsed_prereleases = [], []
    for tag_name in api_tags:
        parsed = parse_semver(tag_name)
        if parsed:
            if PACKAGING_AVAILABLE and packaging_version and isinstance(parsed, packaging_version.Version):
                if not parsed.is_prerelease: parsed_versions.append(parsed)
                else: parsed_prereleases.append(parsed)
            elif isinstance(parsed, str): # Regex fallback result
                 parsed_versions.append(parsed)

    if parsed_versions:
        try:
            latest_version = sorted(parsed_versions)[-1]
            return str(latest_version)
        except TypeError as te:
            logger_instance.warning(
                f"TypeError while sorting versions for {repo_data.get('name')}: {te}. "
                f"Versions list (first 5 elements): {[str(v) for v in parsed_versions[:5]]}. "
                f"Types in list (first 5 elements): {[type(v).__name__ for v in parsed_versions[:5]]}. "
                "Returning first parsed version if available.",
                extra={'org_group': repo_org_group_context}
            ) # Catch if list contains mixed types (e.g. Version and str)
            return str(parsed_versions[0]) if parsed_versions else "N/A"
    if parsed_prereleases and PACKAGING_AVAILABLE and packaging_version: 
        try:
            latest_prerelease = sorted(parsed_prereleases)[-1]
            return str(latest_prerelease)
        except TypeError:
             logger_instance.warning(f"Could not sort pre-releases for {repo_data.get('name')}. Returning first parsed pre-release if available.", extra={'org_group': repo_org_group_context})
             return str(parsed_prereleases[0]) if parsed_prereleases else "N/A"

    logger_instance.debug(f"No suitable semantic version in tags for {repo_data.get('name')}")
    return "N/A"

def infer_status(repo_data: Dict[str, Any], logger_instance: logging.Logger) -> str:
    repo_name_for_log = f"{repo_data.get('organization', '?')}/{repo_data.get('name', '?')}"
    # repo_name_for_log is already in 'org/repo' format, suitable for org_group
    if repo_data.get('archived', False): 
        logger_instance.debug(f"Status for {repo_name_for_log}: 'archived' (API flag)", extra={'org_group': repo_name_for_log})
        return "archived"
    status_from_readme = repo_data.get('_status_from_readme') 
    if status_from_readme and status_from_readme in VALID_README_STATUSES:
        logger_instance.debug(f"Status for {repo_name_for_log}: '{status_from_readme}' (README)", extra={'org_group': repo_name_for_log})
        return status_from_readme
    last_modified_str = repo_data.get('date', {}).get('lastModified')
    if last_modified_str:
        try:
            last_modified_dt = datetime.fromisoformat(last_modified_str.replace('Z', '+00:00'))
            if last_modified_dt.tzinfo is None:
                 last_modified_dt = last_modified_dt.replace(tzinfo=timezone.utc)
            if (datetime.now(timezone.utc) - last_modified_dt) > timedelta(days=INACTIVITY_THRESHOLD_YEARS * 365.25):
                logger_instance.debug(f"Status for {repo_name_for_log}: 'inactive' (> {INACTIVITY_THRESHOLD_YEARS} years)", extra={'org_group': repo_name_for_log})
                return "inactive"
        except ValueError:
            logger_instance.warning(f"Could not parse lastModified date string '{last_modified_str}' for {repo_name_for_log}.", extra={'org_group': repo_name_for_log})
        except Exception as e:
            logger_instance.error(f"Date comparison error for {repo_name_for_log}: {e}", exc_info=True, extra={'org_group': repo_name_for_log})

    logger_instance.debug(f"Status for {repo_name_for_log}: 'development' (default)")
    return "development"

def _finalize_identifiers_and_urls(
    repo_data_item: Dict[str, Any],
    cfg: Config,
    repo_id_mapping_mgr: RepoIdMappingManager,
    exemption_mgr: ExemptionLogger,
    platform: str,
    target_logger: logging.Logger
) -> None:
    """Handles PrivateID generation, URL updates based on exemption, and logs exemptions."""
    repo_name = repo_data_item.get('name', 'UnknownRepo')
    org_name = repo_data_item.get('organization', 'UnknownOrg')
    repo_org_group_context = f"{org_name}/{repo_name}"

    is_private_or_internal = repo_data_item.get('repositoryVisibility', '').lower() in ['private', 'internal']
    platform_repo_id = str(repo_data_item.get('repo_id', '')) # Ensure it's a string for prefixing
    prefixed_repo_id_for_code_json = None

    if is_private_or_internal and platform_repo_id:
        private_emails_data = repo_data_item.get('_private_contact_emails', [])
        contact_emails_for_csv = ';'.join(filter(None, private_emails_data)) if isinstance(private_emails_data, list) else ''
        
        prefixed_repo_id_for_code_json = repo_id_mapping_mgr.get_or_create_mapping_entry(
            platform_repo_id=platform_repo_id,
            organization=org_name,
            repo_name=repo_name,
            repository_url=repo_data_item.get('repositoryURL', ''),
            contact_emails_str_arg=contact_emails_for_csv,
            platform_prefix=platform.lower()
        )
        repo_data_item['privateID'] = prefixed_repo_id_for_code_json
    else:
        repo_data_item.pop('privateID', None)

    usage_type = repo_data_item.get('permissions', {}).get('usageType')
    is_exempt = usage_type and usage_type.lower().startswith('exempt')

    if is_private_or_internal:
        if is_exempt and cfg.EXEMPTED_NOTICE_URL:
            repo_data_item['repositoryURL'] = cfg.EXEMPTED_NOTICE_URL
        elif cfg.INSTRUCTIONS_URL:
            repo_data_item['repositoryURL'] = cfg.INSTRUCTIONS_URL
    
    if is_exempt:
        log_id_for_exemption = prefixed_repo_id_for_code_json or \
                                 (f"NoPlatformRepoID-{platform.lower()}-{org_name}-{repo_name}" if is_private_or_internal else f"Public-{org_name}-{repo_name}")
        exemption_mgr.log_exemption(
            log_id_for_exemption,
            repo_name,
            usage_type,
            repo_data_item.get('permissions', {}).get('exemptionText', '')
        )

def _finalize_status_version_dates(repo_data_item: Dict[str, Any], target_logger: logging.Logger) -> None:
    """Infers status, version, and formats dates."""
    repo_data_item['status'] = infer_status(repo_data_item, target_logger)
    if repo_data_item.get('version', 'N/A') == 'N/A':
        repo_data_item['version'] = infer_version(repo_data_item, target_logger)

    if 'date' in repo_data_item and isinstance(repo_data_item['date'], dict):
        for key, value in list(repo_data_item['date'].items()):
            if isinstance(value, datetime):
                repo_data_item['date'][key] = value.isoformat()
            elif value is None:
                repo_data_item['date'].pop(key, None)
        if not repo_data_item['date']:
            repo_data_item.pop('date', None)

def _cleanup_final_repo_data(repo_data_item: Dict[str, Any]) -> Dict[str, Any]:
    """Removes temporary/internal fields and cleans None values for final output."""
    repo_data_item.pop('_api_tags', None)
    repo_data_item.pop('archived', None)
    repo_data_item.pop('_status_from_readme', None)
    repo_data_item.pop('_is_generic_organization', None)

    final_cleaned_item = {}
    for k, v_item in repo_data_item.items():
        if v_item is None:
            continue
        if isinstance(v_item, dict):
            cleaned_v_item = {nk: nv for nk, nv in v_item.items() if nv is not None}
            if cleaned_v_item:
                final_cleaned_item[k] = cleaned_v_item
        elif isinstance(v_item, list):
            cleaned_list_item = [item for item in v_item if item is not None]
            if cleaned_list_item:
                final_cleaned_item[k] = cleaned_list_item
        else:
            final_cleaned_item[k] = v_item
    return final_cleaned_item

def process_and_finalize_repo_data_list(
    repos_list: List[Dict[str, Any]], 
    cfg: Config, 
    repo_id_mapping_mgr: RepoIdMappingManager,
    exemption_mgr: ExemptionLogger, 
    target_logger: logging.Logger,
    platform: str # Added platform parameter
) -> List[Dict[str, Any]]:
    """
    Processes a list of repository data dictionaries, applying exemptions,
    generating private IDs, inferring status/version, and cleaning data.
    """
    finalized_list = []
    if not repos_list:
        return []

    for repo_data_item in repos_list:
        if not isinstance(repo_data_item, dict):
            target_logger.warning(f"Skipping non-dictionary item in repos_list: {type(repo_data_item)}")
            continue

        repo_name = repo_data_item.get('name', 'UnknownRepo')
        org_name = repo_data_item.get('organization', 'UnknownOrg')
        repo_org_group_context = f"{org_name}/{repo_name}"
        
        target_logger.debug(f"Finalizing data for repo: {org_name}/{repo_name}", extra={'org_group': repo_org_group_context})

        if 'processing_error' in repo_data_item:
            target_logger.error(f"Skipping finalization for {org_name}/{repo_name} due to previous error: {repo_data_item['processing_error']}", extra={'org_group': repo_org_group_context})
            # Include essential fields even for errored items for traceability
            finalized_list.append({
                "name": repo_name, 
                "organization": org_name, 
                "_azure_project_name": repo_data_item.get("_azure_project_name"), # Keep if ADO
                "processing_error": repo_data_item['processing_error']
            })
            continue
        
        try:
            _finalize_identifiers_and_urls(repo_data_item, cfg, repo_id_mapping_mgr, exemption_mgr, platform, target_logger)

            # --- Organization Finalization ---
            # _is_generic_organization flag is set by exemption_processor
            if repo_data_item.get('_is_generic_organization', False):
                repo_data_item['organization'] = cfg.AGENCY_NAME

            _finalize_status_version_dates(repo_data_item, target_logger)
            final_cleaned_item = _cleanup_final_repo_data(repo_data_item)
            finalized_list.append(final_cleaned_item)

        except Exception as e:
            target_logger.error(f"Error during final processing for {org_name}/{repo_name}: {e}", exc_info=True, extra={'org_group': repo_org_group_context})
            finalized_list.append({
                "name": repo_name, 
                "organization": org_name, 
                "_azure_project_name": repo_data_item.get("_azure_project_name"),
                "processing_error": f"Finalization stage: {e}"
            })
            
    return finalized_list

# --- CLI Argument Parsing Helpers ---
# These are used by generate_codejson.py's main_cli
def get_targets_from_cli_or_env(cli_arg_value: Optional[str], env_config_value: List[str], entity_name_plural: str, main_logger: logging.Logger) -> List[str]: 
    targets = []
    source = ""
    if cli_arg_value:
        main_logger.info(f"CLI override: Using {entity_name_plural} from command line: '{cli_arg_value}'")
        targets = [item.strip() for item in cli_arg_value.split(',') if item.strip()]
        source = "CLI"
    elif env_config_value:
        main_logger.info(f"Using {entity_name_plural} from .env: {', '.join(env_config_value)}")
        targets = env_config_value
        source = ".env"
    if not targets:
        main_logger.info(f"No {entity_name_plural} specified via {source if source else 'command line or .env'} to scan.")
    return targets

def parse_azure_targets_from_string_list(raw_target_list: List[str], default_org_from_env: Optional[str], main_logger: logging.Logger) -> List[str]: 
    parsed_targets = []
    for target_str in raw_target_list:
        if '/' in target_str:
            org, proj = target_str.split('/', 1)
            parsed_targets.append(f"{org.strip()}/{proj.strip()}") 
        elif target_str: 
            if default_org_from_env and default_org_from_env != "YourAzureDevOpsOrgName": # Check against placeholder
                main_logger.info(f"Azure target '{target_str}' assumes default org '{default_org_from_env}'.")
                parsed_targets.append(f"{default_org_from_env}/{target_str.strip()}")
            else:
                main_logger.warning(f"Azure target '{target_str}' is not in Org/Project format and no valid default AZURE_DEVOPS_ORG_ENV is set. Skipping.")
    return parsed_targets

# --- Helper for Time Formatting ---
def format_duration(total_seconds: float) -> str:
    """Converts total seconds into a string of hours, minutes, and rounded seconds."""
    if total_seconds < 0:
        return "0 seconds"
    hours = int(total_seconds // 3600)
    remaining_seconds_after_hours = total_seconds % 3600
    minutes = int(remaining_seconds_after_hours // 60)
    final_seconds_float = remaining_seconds_after_hours % 60
    rounded_seconds = int(round(final_seconds_float))

    if rounded_seconds == 60:
        rounded_seconds = 0
        minutes += 1
        if minutes == 60:
            minutes = 0
            hours += 1

    parts = []
    if hours > 0: parts.append(f"{hours} hour{'s' if hours != 1 else ''}")
    if minutes > 0: parts.append(f"{minutes} minute{'s' if minutes != 1 else ''}")
    if rounded_seconds > 0 or not parts or (hours > 0 or minutes > 0):
        parts.append(f"{rounded_seconds} second{'s' if rounded_seconds != 1 else ''}")
    
    return ", ".join(parts) if parts else "0 seconds"
