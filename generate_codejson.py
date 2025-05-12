# generate_codejson.py
"""
Main script for the Share IT Act Repository Scanning Tool.

This script orchestrates the process of scanning repositories across multiple
platforms (GitHub, GitLab, Azure DevOps), processing the collected metadata,
and generating a `code.json` file compliant with the code.gov schema v2.0.
"""

import os
import json
import logging
import logging.handlers
import time
import re
import argparse
import sys
import glob
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv # Still useful for non-auth configs
from typing import List, Optional, Dict, Any

# Import connectors - Ensure these files exist and are importable
try:
    import clients.github_connector
    import clients.gitlab_connector
    import clients.azure_devops_connector
except ImportError as e:
    print(f"Error importing connector modules: {e}")
    print("Please ensure 'clients' directory exists and contains github_connector.py, gitlab_connector.py, and azure_devops_connector.py")
    sys.exit(1)

# Import utils - Ensure these files exist and are importable
try:
    from utils import ExemptionLogger, RepoIdMappingManager # Updated import
except ImportError as e:
    print(f"Error importing utility modules: {e}")
    print("Please ensure 'utils' directory exists and contains ExemptionLogger.py and repo_id_mapping_manager.py") # Updated filename
    sys.exit(1)

# --- Check for packaging library for version parsing ---
PACKAGING_AVAILABLE = False
try:
    import packaging.version as packaging_version
    PACKAGING_AVAILABLE = True
    logging.getLogger().info("Using 'packaging' library for version parsing.")
except ImportError:
    logging.getLogger().warning("Optional library 'packaging' not found. Version parsing will use basic regex (less reliable). Install with: pip install packaging")

# --- Constants ---
INACTIVITY_THRESHOLD_YEARS = 2
VALID_README_STATUSES = {'maintained', 'deprecated', 'experimental', 'active', 'inactive'}
LOG_DIR_NAME = "logs"
INTERMEDIATE_FILE_PATTERN = "intermediate_*.json"
CODE_JSON_SCHEMA_VERSION = "2.0"
CODE_JSON_MEASUREMENT_TYPE = {"method": "projects"}

# --- Configuration Class ---
class Config:
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
        self.PRIVATE_ID_FILEPATH = os.path.join(self.OUTPUT_DIR, self.PRIVATE_ID_FILENAME) # Reverted path

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

# --- Logging Setup ---
def setup_global_logging(log_level=logging.INFO):
    log_directory = "logs"
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
    log_dir = os.path.join(output_dir, LOG_DIR_NAME)
    os.makedirs(log_dir, exist_ok=True)
    log_file_path = os.path.join(log_dir, log_file_name)

    logger = logging.getLogger(logger_name)
    logger.setLevel(level)
    
    if logger.hasHandlers():
        for handler in list(logger.handlers):
            logger.removeHandler(handler)
            handler.close()
    logger.propagate = False

    fh = logging.FileHandler(log_file_path, mode='w', encoding='utf-8')
    fh.setLevel(level)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
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
    logger = logging.getLogger(__name__)
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
            logger.info(f"Backed up existing '{filename}' to '{backup_filename}'.")
        except Exception as e:
            logger.error(f"Error backing up '{filename}': {e}", exc_info=True)

# --- Data Processing and Inference ---
def parse_semver(tag_name):
    if not tag_name or not isinstance(tag_name, str): return None
    cleaned_tag = re.sub(r'^(v|release-|Release-|jenkins-\S+-)', '', tag_name.strip())
    if PACKAGING_AVAILABLE:
        try:
            return packaging_version.parse(cleaned_tag)
        except packaging_version.InvalidVersion:
            return None
    else:
        if re.match(r'^\d+\.\d+(\.\d+)?($|[.-])', cleaned_tag):
             return cleaned_tag
        return None

def infer_version(repo_data, logger_instance):
    api_tags = repo_data.get('_api_tags', []) 
    if not api_tags: return "N/A"
    parsed_versions, parsed_prereleases = [], []
    for tag_name in api_tags:
        parsed = parse_semver(tag_name)
        if parsed:
            if PACKAGING_AVAILABLE:
                if not parsed.is_prerelease: parsed_versions.append(parsed)
                else: parsed_prereleases.append(parsed)
            else: # Regex fallback
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
                "Returning first parsed version if available."
            )
            return str(parsed_versions[0]) if parsed_versions else "N/A"
    if parsed_prereleases and PACKAGING_AVAILABLE: 
        try:
            latest_prerelease = sorted(parsed_prereleases)[-1]
            return str(latest_prerelease)
        except TypeError:
             logger_instance.warning(f"Could not sort pre-releases for {repo_data.get('name')}. Returning first parsed pre-release if available.")
             return str(parsed_prereleases[0]) if parsed_prereleases else "N/A"

    logger_instance.debug(f"No suitable semantic version in tags for {repo_data.get('name')}")
    return "N/A"

def infer_status(repo_data, logger_instance):
    repo_name_for_log = f"{repo_data.get('organization', '?')}/{repo_data.get('name', '?')}"
    if repo_data.get('archived', False): 
        logger_instance.debug(f"Status for {repo_name_for_log}: 'archived' (API flag)")
        return "archived"
    status_from_readme = repo_data.get('_status_from_readme') 
    if status_from_readme and status_from_readme in VALID_README_STATUSES:
        logger_instance.debug(f"Status for {repo_name_for_log}: '{status_from_readme}' (README)")
        return status_from_readme
    last_modified_str = repo_data.get('date', {}).get('lastModified')
    if last_modified_str:
        try:
            last_modified_dt = datetime.fromisoformat(last_modified_str.replace('Z', '+00:00'))
            if last_modified_dt.tzinfo is None:
                 last_modified_dt = last_modified_dt.replace(tzinfo=timezone.utc)
            if (datetime.now(timezone.utc) - last_modified_dt) > timedelta(days=INACTIVITY_THRESHOLD_YEARS * 365.25):
                logger_instance.debug(f"Status for {repo_name_for_log}: 'inactive' (> {INACTIVITY_THRESHOLD_YEARS} years)")
                return "inactive"
        except ValueError:
            logger_instance.warning(f"Could not parse lastModified date string '{last_modified_str}' for {repo_name_for_log}.")
        except Exception as e:
            logger_instance.error(f"Date comparison error for {repo_name_for_log}: {e}", exc_info=True)

    logger_instance.debug(f"Status for {repo_name_for_log}: 'development' (default)")
    return "development"

def process_and_finalize_repo_data_list(
    repos_list: List[Dict[str, Any]], 
    cfg: Config, 
    repo_id_mapping_mgr: RepoIdMappingManager, # Updated parameter name
    exemption_mgr: ExemptionLogger, 
    target_logger: logging.Logger,
    platform: str # Added platform for prefixing
) -> List[Dict[str, Any]]:
    finalized_list = []
    if not repos_list:
        return []

    for repo_data in repos_list:
        repo_name = repo_data.get('name', 'UnknownRepo')
        org_name = repo_data.get('organization', 'UnknownOrg')
        is_private_or_internal = repo_data.get('repositoryVisibility', '').lower() in ['private', 'internal']
        platform_repo_id = str(repo_data.get('repo_id', '')) # Get the platform's repo_id

        target_logger.debug(f"Finalizing data for repo: {org_name}/{repo_name}")

        if 'processing_error' in repo_data:
            target_logger.error(f"Skipping finalization for {org_name}/{repo_name} due to previous error: {repo_data['processing_error']}")
            finalized_list.append({
                "name": repo_name,
                "organization": org_name,
                "processing_error": repo_data['processing_error']
            })
            continue

        try:
            prefixed_repo_id_for_code_json = None
            if is_private_or_internal and platform_repo_id:
                private_emails_list = repo_data.get('_private_contact_emails', [])
                # Ensure entry in mapping file (stores raw platform_repo_id and URL)
                prefixed_repo_id_for_code_json = f"{platform.lower()}_{platform_repo_id}" # Create prefixed ID first
                repo_id_mapping_mgr.get_or_create_mapping_entry(
                    private_id_value=prefixed_repo_id_for_code_json, # Pass the prefixed ID
                    repo_name=repo_name, 
                    organization=org_name, 
                    repository_url=repo_data.get('repositoryURL', ''), # Pass the URL
                    contact_emails=private_emails_list
                )
                
                # Create prefixed ID for code.json's privateID field
                repo_data['privateID'] = prefixed_repo_id_for_code_json 
            else:
                if is_private_or_internal and not platform_repo_id:
                    target_logger.warning(f"Repo {org_name}/{repo_name} is private/internal but has no platform_repo_id. Cannot set 'privateID' field or map.")
                repo_data.pop('privateID', None) # Remove if not private/internal or no repo_id

            usage_type = repo_data.get('permissions', {}).get('usageType')
            is_exempt = usage_type and usage_type.lower().startswith('exempt')

            if is_private_or_internal:
                if is_exempt and cfg.EXEMPTED_NOTICE_URL:
                    repo_data['repositoryURL'] = cfg.EXEMPTED_NOTICE_URL
                    target_logger.debug(f"Private/Internal & Exempt repo {repo_name}: Using EXEMPTED_NOTICE_URL for repositoryURL.")
                elif cfg.INSTRUCTIONS_URL:
                    repo_data['repositoryURL'] = cfg.INSTRUCTIONS_URL
                    target_logger.debug(f"Private/Internal repo {repo_name}: Using INSTRUCTIONS_URL for repositoryURL.")
                else:
                    target_logger.warning(f"Private/Internal repo {repo_name}: Neither EXEMPTED_NOTICE_URL (if exempt) nor INSTRUCTIONS_URL is set. Actual repo URL will be used (may expose internal path).")
            
            if is_exempt:
                exemption_text = repo_data.get('permissions', {}).get('exemptionText', '')
                # Log exemption with the prefixed_repo_id if available, otherwise a placeholder
                log_id_for_exemption = prefixed_repo_id_for_code_json
                if not log_id_for_exemption: 
                    if is_private_or_internal: # Fallback if private/internal but no platform_repo_id
                         log_id_for_exemption = f"NoPlatformRepoID-{platform.lower()}-{org_name}-{repo_name}"
                    # For public, no privateID is set, so no specific ID needed for exemption log unless desired
                exemption_mgr.log_exemption(log_id_for_exemption or f"Public-{org_name}-{repo_name}", repo_name, usage_type, exemption_text)

            repo_data['status'] = infer_status(repo_data, target_logger)
            if repo_data.get('version', 'N/A') == 'N/A':
                 repo_data['version'] = infer_version(repo_data, target_logger)

            if 'date' in repo_data and isinstance(repo_data['date'], dict):
                for key, value in list(repo_data['date'].items()):
                    if isinstance(value, datetime):
                        repo_data['date'][key] = value.isoformat()
                    elif value is None:
                         repo_data['date'].pop(key)
                if not repo_data['date']:
                    repo_data.pop('date')

            repo_data.pop('_private_contact_emails', None)
            repo_data.pop('_api_tags', None)
            repo_data.pop('archived', None) 
            repo_data.pop('_status_from_readme', None) 

            cleaned_repo_data = {}
            for k, v in repo_data.items():
                if v is None:
                    continue 
                if isinstance(v, dict):
                    cleaned_v = {nk: nv for nk, nv in v.items() if nv is not None}
                    if cleaned_v:
                        cleaned_repo_data[k] = cleaned_v
                elif isinstance(v, list):
                     cleaned_list = [item for item in v if item is not None]
                     if cleaned_list: # Only add if list is not empty after cleaning
                        cleaned_repo_data[k] = cleaned_list
                else:
                    cleaned_repo_data[k] = v
            finalized_list.append(cleaned_repo_data)
        except Exception as e:
            target_logger.error(f"Error during final processing for {org_name}/{repo_name}: {e}", exc_info=True)
            finalized_list.append({
                "name": repo_name,
                "organization": org_name,
                "processing_error": f"Finalization stage: {e}"
            })
    return finalized_list

# --- Core Scanning and Merging Functions ---
def scan_and_process_single_target(
    platform: str, 
    target_identifier: str, 
    cfg: Config, 
    repo_id_mapping_mgr: RepoIdMappingManager, # Updated parameter name
    exemption_mgr: ExemptionLogger, 
    global_repo_counter: List[int], 
    limit_to_pass: Optional[int], 
    auth_params: Dict[str, Any], 
    platform_url: Optional[str] = None,
    hours_per_commit: Optional[float] = None 
) -> bool:
    target_logger_name = f"{platform}.{target_identifier.replace('/', '_').replace('.', '_')}"
    target_log_filename = f"{platform}_{target_identifier.replace('/', '_').replace('.', '_')}.log"
    target_logger = setup_target_logger(target_logger_name, target_log_filename, cfg.OUTPUT_DIR)
    
    target_logger.info(f"--- Starting scan for {platform} target: {target_identifier} ---")
    
    fetched_repos = []
    connector_success = False 

    if limit_to_pass is not None and global_repo_counter[0] >= limit_to_pass:
        target_logger.warning(f"Global debug limit ({limit_to_pass}) reached. Skipping scan for {target_identifier}.")
        target_logger.info(f"--- Finished scan for {platform} target: {target_identifier} (Skipped due to limit) ---")
        return True 

    try:
        if platform == "github":
            fetched_repos = clients.github_connector.fetch_repositories(
                token=auth_params.get("token"), 
                org_name=target_identifier, 
                processed_counter=global_repo_counter, 
                debug_limit=limit_to_pass, 
                github_instance_url=platform_url,
                hours_per_commit=hours_per_commit         
            )
            connector_success = True
        elif platform == "gitlab":
            fetched_repos = clients.gitlab_connector.fetch_repositories(
                token=auth_params.get("token"), 
                group_path=target_identifier, 
                processed_counter=global_repo_counter, 
                debug_limit=limit_to_pass, 
                gitlab_instance_url=platform_url, # Corrected param name
                hours_per_commit=hours_per_commit          
            )
            connector_success = True
        elif platform == "azure":
            if '/' not in target_identifier:
                 target_logger.error(f"Invalid Azure DevOps target format: '{target_identifier}'. Expected Org/Project.")
                 return False
            org_name, project_name = target_identifier.split('/', 1)
            fetched_repos = clients.azure_devops_connector.fetch_repositories(
                pat_token=auth_params.get("pat_token"),
                spn_client_id=auth_params.get("spn_client_id"),
                spn_client_secret=auth_params.get("spn_client_secret"),
                spn_tenant_id=auth_params.get("spn_tenant_id"),
                organization_name=org_name, 
                project_name=project_name, 
                processed_counter=global_repo_counter, 
                debug_limit=limit_to_pass,
                hours_per_commit=hours_per_commit          
            )
            connector_success = True
        else:
            target_logger.error(f"Unknown platform: {platform}")
            return False
        
        if fetched_repos:
            target_logger.info(f"Connector returned {len(fetched_repos)} repositories for {target_identifier}.")
        else:
            target_logger.info(f"Connector returned no repositories for {target_identifier}.")

    except Exception as e:
        target_logger.critical(f"Critical error during {platform} connector execution for {target_identifier}: {e}", exc_info=True)
        target_logger.info(f"--- Finished scan for {platform} target: {target_identifier} (Critical Connector Error) ---")
        return False 

    default_org_ids_for_exemption_processor = [target_identifier] 
    if platform == "azure" and '/' in target_identifier:
        default_org_ids_for_exemption_processor = [target_identifier.split('/',1)[0]]


    if not fetched_repos and connector_success :
        target_logger.info(f"No repositories to process for {target_identifier} or limit reached within connector.")
        intermediate_data = [] 
    else:
        target_logger.info(f"Finalizing {len(fetched_repos)} repositories for {target_identifier}...")
        intermediate_data = process_and_finalize_repo_data_list(
            [repo for repo in fetched_repos if repo is not None and not repo.get("processing_error")], 
            cfg, repo_id_mapping_mgr, exemption_mgr, target_logger, platform # Pass platform
        )
        errored_repos = [repo for repo in fetched_repos if repo and repo.get("processing_error")]
        if errored_repos:
            intermediate_data.extend(errored_repos)


    intermediate_filename = f"intermediate_{platform}_{target_identifier.replace('/', '_').replace('.', '_')}.json"
    intermediate_filepath = os.path.join(cfg.OUTPUT_DIR, intermediate_filename)
    
    if write_json_file(intermediate_data, intermediate_filepath):
        target_logger.info(f"Successfully wrote intermediate data to {intermediate_filepath}")
        target_logger.info(f"--- Finished scan for {platform} target: {target_identifier} ---")
        return True 
    else:
        target_logger.error(f"Failed to write intermediate data for {target_identifier}.")
        target_logger.info(f"--- Finished scan for {platform} target: {target_identifier} (Write Error) ---")
        return False 

def merge_intermediate_catalogs(cfg: Config, main_logger: logging.Logger) -> bool: # Removed repo_id_mapping_mgr
    main_logger.info("--- Starting Merge Operation ---")
    search_path = os.path.join(cfg.OUTPUT_DIR, INTERMEDIATE_FILE_PATTERN)
    intermediate_files = glob.glob(search_path)

    if not intermediate_files:
        main_logger.info(f"No intermediate catalog files found matching '{search_path}'. Nothing to merge.")
        return True 

    main_logger.info(f"Found {len(intermediate_files)} intermediate catalog files to merge.")
    all_projects = []
    merge_errors = False 

    for filepath in intermediate_files:
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, list):
                    all_projects.extend(data)
                else:
                    main_logger.warning(f"Content of {filepath} is not a list. Skipping.")
                    merge_errors = True 
        except json.JSONDecodeError:
            main_logger.error(f"Error decoding JSON from {filepath}. Skipping.", exc_info=True)
            merge_errors = True 
        except Exception as e:
            main_logger.error(f"Unexpected error processing {filepath}: {e}. Skipping.", exc_info=True)
            merge_errors = True 
    
    if not all_projects and intermediate_files:
        main_logger.warning("No data collected from intermediate files, though files were present.")
        return not merge_errors
    
    final_code_json_structure = {
        "version": CODE_JSON_SCHEMA_VERSION,
        "agency": cfg.AGENCY_NAME, 
        "measurementType":  CODE_JSON_MEASUREMENT_TYPE, 
        "projects": [] # Initialize projects as an empty list
    }
    
    processed_projects_for_final_catalog = []
    for project_data in all_projects:
        now_iso = datetime.now(timezone.utc).isoformat() # For metadataLastUpdated
        # Skip if it's just an error placeholder from a failed connector run for a target
        if "processing_error" in project_data and len(project_data.keys()) <= 3: # e.g. name, org, error
            main_logger.warning(f"Skipping project entry during merge due to processing_error: {project_data.get('name', 'Unknown')}")
            processed_projects_for_final_catalog.append(project_data) # Keep error record
            continue

        updated_project_data = project_data.copy() # Work with a copy

        # Add/Update metadataLastUpdated
        if "date" not in updated_project_data or not isinstance(updated_project_data.get("date"), dict):
            updated_project_data["date"] = {}
        updated_project_data["date"]["metadataLastUpdated"] = now_iso

        # --- Visibility Normalization for Final Output ---
        # The usageType should have been correctly set by exemption_processor.py
        # and is trusted from the intermediate file.
        # Here, we just ensure 'internal' visibility is mapped to 'private' for the final code.json.
        repo_visibility_original = updated_project_data.get("repositoryVisibility", "").lower()
        if repo_visibility_original == "internal":
            updated_project_data["repositoryVisibility"] = "private" 
            main_logger.debug(f"Repo {updated_project_data.get('name')}: Standardized visibility from 'internal' to 'private' for final output.")
        
        # The 'privateID' field (now containing prefixed repo_id) comes from the intermediate file.
        # No need to generate or modify it here.
        main_logger.debug(f"Repo {updated_project_data.get('name')}: Using privateID '{updated_project_data.get('privateID')}' from intermediate file.")
        
        processed_projects_for_final_catalog.append(updated_project_data)

    backup_existing_file(cfg.OUTPUT_DIR, cfg.CATALOG_JSON_FILE)
    
    final_code_json_structure["projects"] = processed_projects_for_final_catalog
    final_code_json_structure["projects"].sort(key=lambda x: x.get("name", "").lower()) # Sort projects by name

    final_catalog_filepath = os.path.join(cfg.OUTPUT_DIR, cfg.CATALOG_JSON_FILE)
    if write_json_file(final_code_json_structure, final_catalog_filepath):
        main_logger.info(f"Successfully merged {len(processed_projects_for_final_catalog)} projects into {final_catalog_filepath}")
        main_logger.info("--- Merge Operation Finished Successfully ---")
        return not merge_errors 
    else:
        main_logger.error(f"Failed to write final merged catalog to {final_catalog_filepath}")
        main_logger.info("--- Merge Operation Finished With Errors ---")
        return False 

# --- CLI Argument Parsing Helpers ---
def get_targets_from_cli_or_env(cli_arg_value: Optional[str], env_config_value: List[str], entity_name_plural: str, main_logger: logging.Logger) -> List[str]: 
    targets = []
    source = ""
    if cli_arg_value:
        main_logger.info(f"CLI override: Using {entity_name_plural} from command line: '{cli_arg_value}'")
        targets = [item.strip() for item in cli_arg_value.split(',') if item.strip()]
        source = "CLI"
    elif env_config_value: # Only use .env if CLI arg is not provided
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
            if default_org_from_env and default_org_from_env != "YourAzureDevOpsOrgName":
                main_logger.info(f"Azure target '{target_str}' assumes default org '{default_org_from_env}'.")
                parsed_targets.append(f"{default_org_from_env}/{target_str.strip()}")
            else:
                main_logger.warning(f"Azure target '{target_str}' is not in Org/Project format and no default AZURE_DEVOPS_ORG_ENV is set. Skipping.")
    return parsed_targets

# --- CLI Helper for Token Validation ---
def _validate_cli_pat_token(token_value: Optional[str], cli_arg_name: str, platform_name: str, placeholder_check_func, main_logger: logging.Logger) -> str:
    if not token_value:
        main_logger.error(f"--{cli_arg_name} ({platform_name} PAT) is required for {platform_name} scans.")
        sys.exit(1)
    if placeholder_check_func(token_value):
        main_logger.error(f"{platform_name} PAT provided via --{cli_arg_name} is a placeholder. Cannot scan {platform_name}.")
        sys.exit(1)
    return token_value

# --- Main CLI Function ---
def main_cli(): 
    script_start_time = time.time() # Record the start time of the script

    cfg = Config()
    setup_global_logging()
    main_logger = logging.getLogger(__name__) 

    parser = argparse.ArgumentParser(description="Share IT Act Repository Scanning Tool")
    subparsers = parser.add_subparsers(dest="command", help="Available commands.", required=True)

    # --- GitHub Command ---
    gh_parser = subparsers.add_parser("github", help="Scan GitHub organizations.")
    gh_parser.add_argument("--orgs", help="Comma-separated organizations to scan. Used for public GitHub.com by default, or for GHES if --github-ghes-url is also specified. Overrides .env GITHUB_ORGS_ENV for public GitHub if --github-ghes-url is not used.")
    gh_parser.add_argument("--github-ghes-url", help="URL of the GitHub Enterprise Server instance (e.g., https://github.mycompany.com). If provided, --orgs will target this GHES instance.")
    gh_parser.add_argument("--gh-tk", help="GitHub Personal Access Token (PAT).")
    gh_parser.add_argument("--limit", type=int, help="Limit total repositories processed for this GitHub scan run (overrides .env).")
    gh_parser.add_argument("--hours-per-commit", type=float, help="Hours to estimate per commit for GitHub repos. Enables labor estimation.")

    # --- GitLab Command ---
    gl_parser = subparsers.add_parser("gitlab", help="Scan configured GitLab groups.")
    gl_parser.add_argument("--groups", help="Comma-separated GitLab groups/paths to scan (overrides .env).")
    gl_parser.add_argument("--gitlab-url", help="GitLab instance URL (e.g., https://gitlab.com) (overrides .env).")
    gl_parser.add_argument("--gl-tk", help="GitLab Personal Access Token (PAT).")
    gl_parser.add_argument("--limit", type=int, help="Limit total repositories processed for this GitLab scan run (overrides .env).")
    gl_parser.add_argument("--hours-per-commit", type=float, help="Hours to estimate per commit for GitLab repos. Enables labor estimation.")

    # --- Azure DevOps Command ---
    az_parser = subparsers.add_parser("azure", help="Scan Azure DevOps Org/Project targets.")
    az_parser.add_argument("--targets", help="Comma-separated Azure Org/Project pairs (overrides .env).")
    az_parser.add_argument("--az-tk", help="Azure DevOps Personal Access Token (PAT).")
    az_parser.add_argument("--az-cid", help="Azure Service Principal Client ID.")
    az_parser.add_argument("--az-cs", help="Azure Service Principal Client Secret.")
    az_parser.add_argument("--az-tid", help="Azure Service Principal Tenant ID.")
    az_parser.add_argument("--limit", type=int, help="Limit total repositories processed for this Azure scan run (overrides .env).")
    az_parser.add_argument("--hours-per-commit", type=float, help="Hours to estimate per commit for Azure DevOps repos. Enables labor estimation.")

    # --- Merge Command ---
    merge_parser = subparsers.add_parser("merge", help="Merge intermediate catalog files into the final code.json.")

    args = parser.parse_args()


    os.makedirs(cfg.OUTPUT_DIR, exist_ok=True)
    try:
        exemption_manager = ExemptionLogger(cfg.EXEMPTION_LOG_FILEPATH)
        repo_id_mapping_manager = RepoIdMappingManager(cfg.PRIVATE_ID_FILEPATH) # Use correct filepath from Config
        main_logger.info("ExemptionLogger and RepoIdMappingManager initialized.")
    except Exception as mgr_err:
        main_logger.critical(f"Failed to initialize Exemption/RepoIdMapping managers: {mgr_err}", exc_info=True)
        sys.exit(1) 

    overall_command_success = True 
    global_repo_scan_counter = [0] 

    # Determine repository processing limit (CLI > .env > no limit)
    limit_for_scans = None
    cli_limit_val = getattr(args, 'limit', None)
    if cli_limit_val is not None:
        if cli_limit_val > 0:
            limit_for_scans = cli_limit_val
            main_logger.info(f"CLI override: Repository processing limit set to {limit_for_scans} for this run.")
        else: # limit <= 0 means no limit for this run
            main_logger.info(f"CLI override: --limit set to {cli_limit_val}, effectively no limit for this run (processing all).")
    elif cfg.DEBUG_REPO_LIMIT is not None: 
        limit_for_scans = cfg.DEBUG_REPO_LIMIT
        main_logger.info(f"Using repository processing limit from .env: {limit_for_scans}.")
    else:
        main_logger.info("No repository processing limit set (processing all).")

    # Determine hours_per_commit (CLI > .env > None/Disabled)
    hours_per_commit_for_scan: Optional[float] = None 
    cli_hpc_val = getattr(args, 'hours_per_commit', None)
    if cli_hpc_val is not None:
        try:
            hours_per_commit_for_scan = float(cli_hpc_val)
            # Log message will be printed below if hours_per_commit_for_scan is not None
        except ValueError:
            main_logger.error(f"Invalid --hours-per-commit CLI value '{cli_hpc_val}'. Labor estimation will be DISABLED.")
    elif cfg.HOURS_PER_COMMIT_ENV is not None: 
        hours_per_commit_for_scan = cfg.HOURS_PER_COMMIT_ENV
    
    if hours_per_commit_for_scan is not None:
        main_logger.info(f"Using .env: Labor hours estimation ENABLED. Hours per commit set to {hours_per_commit_for_scan}.")
    else: 
        main_logger.info(f"Labor hours estimation DISABLED for this run (no valid hours_per_commit from CLI or .env).")

    auth_params_for_connector: Dict[str, Any] = {}

    if args.command == "github":
        github_url_for_scan = None
        targets_to_scan = []
        target_entity_name = "Public GitHub.com organizations"
        
        auth_params_for_connector["token"] = _validate_cli_pat_token(
            args.gh_tk, "gh-tk", "GitHub", clients.github_connector.is_placeholder_token, main_logger
        )

        if args.github_ghes_url:
            github_url_for_scan = args.github_ghes_url
            if not args.orgs:
                main_logger.error("--github-ghes-url requires --orgs to be specified for the GHES instance.")
                sys.exit(1)
            targets_to_scan = get_targets_from_cli_or_env(args.orgs, [], "GitHub Enterprise organizations", main_logger)
            main_logger.info(f"Targeting GitHub Enterprise Server: {github_url_for_scan}")
            target_entity_name = f"Organizations on GHES ({github_url_for_scan})"
        else: 
            targets_to_scan = get_targets_from_cli_or_env(args.orgs, cfg.GITHUB_ORGS_ENV, "Public GitHub.com organizations", main_logger)
            main_logger.info("Targeting public GitHub.com")

        if not targets_to_scan: 
            main_logger.info(f"No {target_entity_name} specified to scan.")
            sys.exit(0)
        
        main_logger.info(f"--- Starting GitHub Scan for {len(targets_to_scan)} {target_entity_name} ---")
        for target in targets_to_scan:
            if not scan_and_process_single_target("github", target, cfg, repo_id_mapping_manager, exemption_manager, 
                                                  global_repo_scan_counter, limit_for_scans, 
                                                  auth_params=auth_params_for_connector, platform_url=github_url_for_scan, hours_per_commit=hours_per_commit_for_scan):
                overall_command_success = False 
            if limit_for_scans is not None and global_repo_scan_counter[0] >= limit_for_scans:
                 main_logger.warning(f"Global debug limit ({limit_for_scans}) reached. Stopping further GitHub target scans.")
                 break 
        main_logger.info("--- GitHub Scan Command Finished ---")
    
    elif args.command == "gitlab":
        auth_params_for_connector["token"] = _validate_cli_pat_token(
            args.gl_tk, "gl-tk", "GitLab", clients.gitlab_connector.is_placeholder_token, main_logger
        )

        targets_to_scan = get_targets_from_cli_or_env(args.groups, cfg.GITLAB_GROUPS_ENV, "GitLab groups", main_logger)
        if not targets_to_scan: sys.exit(0)

        gitlab_url_for_scan = args.gitlab_url if args.gitlab_url else cfg.GITLAB_URL_ENV
        if args.gitlab_url:
            main_logger.info(f"CLI override: Using GitLab URL: {args.gitlab_url}")

        main_logger.info(f"--- Starting GitLab Scan for {len(targets_to_scan)} Groups on {gitlab_url_for_scan} ---")
        for target in targets_to_scan:
            if not scan_and_process_single_target("gitlab", target, cfg, repo_id_mapping_manager, exemption_manager, 
                                                  global_repo_scan_counter, limit_for_scans, 
                                                  auth_params=auth_params_for_connector, platform_url=gitlab_url_for_scan, hours_per_commit=hours_per_commit_for_scan):
                overall_command_success = False 
            if limit_for_scans is not None and global_repo_scan_counter[0] >= limit_for_scans:
                 main_logger.warning(f"Global debug limit ({limit_for_scans}) reached. Stopping further GitLab target scans.")
                 break 
        main_logger.info("--- GitLab Scan Command Finished ---")

    elif args.command == "azure":
        if args.az_cid and args.az_cs and args.az_tid:
            main_logger.info("Using Azure Service Principal credentials from CLI.")
            auth_params_for_connector = {
                "spn_client_id": args.az_cid,
                "spn_client_secret": args.az_cs,
                "spn_tenant_id": args.az_tid
            }
            if clients.azure_devops_connector.are_spn_details_placeholders(args.az_cid, args.az_cs, args.az_tid):
                main_logger.error("One or more Azure Service Principal CLI arguments are placeholders. Cannot scan Azure DevOps.")
                sys.exit(1)
        elif args.az_tk:
            main_logger.info("Using Azure PAT from CLI.")
            pat = _validate_cli_pat_token(
                args.az_tk, "az-tk", "Azure DevOps", clients.azure_devops_connector.is_placeholder_token, main_logger
            )
            auth_params_for_connector = {"pat_token": pat}
        else:
            main_logger.error("Azure DevOps scan requires either Service Principal details (--az-cid, --az-cs, --az-tid) or a PAT (--az-tk).")
            sys.exit(1)

        raw_targets_list = []
        if args.targets:
            main_logger.info(f"CLI override: Using Azure DevOps targets from command line: '{args.targets}'")
            raw_targets_list = [t.strip() for t in args.targets.split(',') if t.strip()]
        elif cfg.AZURE_DEVOPS_TARGETS_RAW_ENV:
            main_logger.info(f"Using Azure DevOps targets from .env: {', '.join(cfg.AZURE_DEVOPS_TARGETS_RAW_ENV)}")
            raw_targets_list = cfg.AZURE_DEVOPS_TARGETS_RAW_ENV
        if not raw_targets_list:
            main_logger.info("No Azure DevOps targets specified to scan.")
            sys.exit(0)
        
        targets_to_scan = parse_azure_targets_from_string_list(raw_targets_list, cfg.AZURE_DEVOPS_ORG_ENV, main_logger)
        if not targets_to_scan:
            main_logger.info("No valid Azure DevOps targets to scan after parsing.")
            sys.exit(0)
        
        main_logger.info(f"--- Starting Azure DevOps Scan for {len(targets_to_scan)} Targets ---")
        for target in targets_to_scan: 
            if not scan_and_process_single_target("azure", target, cfg, repo_id_mapping_manager, exemption_manager, 
                                                  global_repo_scan_counter, limit_for_scans, 
                                                  auth_params=auth_params_for_connector, hours_per_commit=hours_per_commit_for_scan):
                overall_command_success = False 
            if limit_for_scans is not None and global_repo_scan_counter[0] >= limit_for_scans:
                 main_logger.warning(f"Global debug limit ({limit_for_scans}) reached. Stopping further Azure DevOps target scans.")
                 break 
        main_logger.info("--- Azure DevOps Scan Command Finished ---")

    elif args.command == "merge":
        backup_existing_file(cfg.OUTPUT_DIR, cfg.EXEMPTION_LOG_FILENAME)
        backup_existing_file(cfg.OUTPUT_DIR, cfg.PRIVATE_ID_FILENAME) # Use correct filename from Config
        if not merge_intermediate_catalogs(cfg, main_logger): # Manager no longer needed here
            overall_command_success = False 
        main_logger.info("--- Merge Command Finished ---")

    try:
        main_logger.info("Saving Exemption logs and Repo ID mappings...")
        exemption_manager.save_all_exemptions() 
        repo_id_mapping_manager.save_all_mappings() # Call save on renamed manager
        main_logger.info(f"Exemptions logged to: {cfg.EXEMPTION_LOG_FILEPATH}")
        main_logger.info(f"Private ID mappings saved to: {cfg.PRIVATE_ID_FILEPATH}") # Update log message
    except Exception as save_err:
        main_logger.error(f"Error saving manager data: {save_err}", exc_info=True)
        overall_command_success = False 

    script_end_time = time.time() # Record the end time of the script
    total_duration_seconds = script_end_time - script_start_time
    main_logger.info(f"Total script execution time: {total_duration_seconds:.2f} seconds.")

    if overall_command_success:
        main_logger.info(f"Command '{args.command}' completed successfully.")
        main_logger.info("----------------------------------------------------------------------")
        sys.exit(0) 
    else:
        main_logger.error(f"Command '{args.command}' encountered errors. Please check logs.")
        main_logger.info("----------------------------------------------------------------------")
        sys.exit(1) 

if __name__ == "__main__":
    main_cli()
