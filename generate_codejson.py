# generate_codejson.py
# c:\src\OCIO-ricky\ShareITAct_RepoScanning\generate_codejson.py
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
    from utils import ExemptionLogger, PrivateIdManager
except ImportError as e:
    print(f"Error importing utility modules: {e}")
    print("Please ensure 'utils' directory exists and contains ExemptionLogger.py and PrivateIdManager.py")
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
        self.PRIVATE_ID_FILEPATH = os.path.join(self.OUTPUT_DIR, self.PRIVATE_ID_FILENAME)

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
            else:
                 parsed_versions.append(parsed)

    if parsed_versions:
        try:
            latest_version = sorted(parsed_versions)[-1]
            return str(latest_version)
        except TypeError:
            logger_instance.warning(f"Could not sort versions for {repo_data.get('name')}. Returning first parsed version if available.")
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

def process_and_finalize_repo_data_list(repos_list: List[Dict[str, Any]], cfg: Config, privateid_mgr: PrivateIdManager, exemption_mgr: ExemptionLogger, target_logger: logging.Logger) -> List[Dict[str, Any]]:
    finalized_list = []
    if not repos_list:
        return []

    for repo_data in repos_list:
        repo_name = repo_data.get('name', 'UnknownRepo')
        org_name = repo_data.get('organization', 'UnknownOrg')
        is_private_or_internal = repo_data.get('repositoryVisibility', '').lower() in ['private', 'internal']

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
            private_emails_list = repo_data.get('_private_contact_emails', [])
            private_id = None
            if is_private_or_internal:
                private_id = privateid_mgr.get_or_generate_id(repo_name, org_name, private_emails_list)
                repo_data['privateID'] = private_id
            else:
                repo_data.pop('privateID', None)

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
                log_id = private_id if is_private_or_internal else f"PublicRepo-{org_name}-{repo_name}"
                exemption_mgr.log_exemption(log_id, repo_name, usage_type, exemption_text)

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
    privateid_mgr: PrivateIdManager, 
    exemption_mgr: ExemptionLogger, 
    global_repo_counter: List[int], 
    limit_to_pass: Optional[int], 
    auth_params: Dict[str, Any], # New parameter for authentication details
    platform_url: Optional[str] = None
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
                token=auth_params.get("token"), # Get token from auth_params
                org_name=target_identifier, 
                processed_counter=global_repo_counter, 
                debug_limit=limit_to_pass, 
                github_instance_url=platform_url
            )
            connector_success = True
        elif platform == "gitlab":
            fetched_repos = clients.gitlab_connector.fetch_repositories(
                token=auth_params.get("token"), # Get token from auth_params
                group_path=target_identifier, 
                processed_counter=global_repo_counter, 
                debug_limit=limit_to_pass, 
                gitlab_instance_url=platform_url 
            )
            connector_success = True
        elif platform == "azure":
            if '/' not in target_identifier:
                 target_logger.error(f"Invalid Azure DevOps target format: '{target_identifier}'. Expected Org/Project.")
                 return False
            org_name, project_name = target_identifier.split('/', 1)
            fetched_repos = clients.azure_devops_connector.fetch_repositories(
                # Pass specific auth params for Azure
                pat_token=auth_params.get("pat_token"),
                spn_client_id=auth_params.get("spn_client_id"),
                spn_client_secret=auth_params.get("spn_client_secret"),
                spn_tenant_id=auth_params.get("spn_tenant_id"),
                organization_name=org_name, 
                project_name=project_name, 
                processed_counter=global_repo_counter, 
                debug_limit=limit_to_pass
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

    default_org_ids_for_exemption_processor = [target_identifier] # Default to the target itself
    if platform == "azure" and '/' in target_identifier:
        # For Azure, target_identifier is "Org/Project", so Org is the primary default
        default_org_ids_for_exemption_processor = [target_identifier.split('/',1)[0]]


    if not fetched_repos and connector_success :
        target_logger.info(f"No repositories to process for {target_identifier} or limit reached within connector.")
        intermediate_data = [] 
    else:
        target_logger.info(f"Finalizing {len(fetched_repos)} repositories for {target_identifier}...")
        # Pass default_org_ids to exemption_processor
        intermediate_data = process_and_finalize_repo_data_list(
            [repo for repo in fetched_repos if repo is not None and not repo.get("processing_error")], # Filter out errored items before finalization
            cfg, privateid_mgr, exemption_mgr, target_logger
        )
        # Handle repos that had processing errors during fetch
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

def merge_intermediate_catalogs(cfg: Config, main_logger: logging.Logger) -> bool:
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
        "version":  CODE_JSON_SCHEMA_VERSION,
        "agency": cfg.AGENCY_NAME, 
        "measurementType":  CODE_JSON_MEASUREMENT_TYPE, 
        "projects": all_projects
    }
    
    now_iso = datetime.now(timezone.utc).isoformat()
    for project in final_code_json_structure["projects"]:
        if "processing_error" not in project:
            if "date" not in project or not isinstance(project.get("date"), dict):
                project["date"] = {}
            project["date"]["metadataLastUpdated"] = now_iso

    backup_existing_file(cfg.OUTPUT_DIR, cfg.CATALOG_JSON_FILE)
    
    final_catalog_filepath = os.path.join(cfg.OUTPUT_DIR, cfg.CATALOG_JSON_FILE)
    if write_json_file(final_code_json_structure, final_catalog_filepath):
        main_logger.info(f"Successfully merged {len(all_projects)} projects into {final_catalog_filepath}")
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

# --- Main CLI Function ---
def main_cli():
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

    # --- GitLab Command ---
    gl_parser = subparsers.add_parser("gitlab", help="Scan configured GitLab groups.")
    gl_parser.add_argument("--groups", help="Comma-separated GitLab groups/paths to scan (overrides .env).")
    gl_parser.add_argument("--gitlab-url", help="GitLab instance URL (e.g., https://gitlab.com) (overrides .env).")
    gl_parser.add_argument("--gl-tk", help="GitLab Personal Access Token (PAT).")
    gl_parser.add_argument("--limit", type=int, help="Limit total repositories processed for this GitLab scan run (overrides .env).")
    
    # --- Azure DevOps Command ---
    az_parser = subparsers.add_parser("azure", help="Scan Azure DevOps Org/Project targets.")
    az_parser.add_argument("--targets", help="Comma-separated Azure Org/Project pairs (overrides .env).")
    az_parser.add_argument("--az-tk", help="Azure DevOps Personal Access Token (PAT).")
    az_parser.add_argument("--az-cid", help="Azure Service Principal Client ID.")
    az_parser.add_argument("--az-cs", help="Azure Service Principal Client Secret.")
    az_parser.add_argument("--az-tid", help="Azure Service Principal Tenant ID.")
    az_parser.add_argument("--limit", type=int, help="Limit total repositories processed for this Azure scan run (overrides .env).")
    
    # --- Merge Command ---
    merge_parser = subparsers.add_parser("merge", help="Merge intermediate catalog files into the final code.json.")

    args = parser.parse_args()

    os.makedirs(cfg.OUTPUT_DIR, exist_ok=True)
    try:
        exemption_manager = ExemptionLogger(cfg.EXEMPTION_LOG_FILEPATH)
        privateid_manager = PrivateIdManager(cfg.PRIVATE_ID_FILEPATH)
        main_logger.info("ExemptionLogger and PrivateIdManager initialized.")
    except Exception as mgr_err:
        main_logger.critical(f"Failed to initialize Exemption/PrivateID managers: {mgr_err}", exc_info=True)
        sys.exit(1) 

    overall_command_success = True 
    global_repo_scan_counter = [0] 

    limit_for_scans = None
    if hasattr(args, 'limit') and args.limit is not None: 
        if args.limit > 0:
            limit_for_scans = args.limit
            main_logger.info(f"CLI override: Repository processing limit set to {limit_for_scans} for this run.")
        else: 
            main_logger.info(f"CLI override: --limit set to {args.limit}, effectively no limit for this run.")
    elif cfg.DEBUG_REPO_LIMIT is not None: 
        limit_for_scans = cfg.DEBUG_REPO_LIMIT
        main_logger.info(f"Using repository processing limit from .env: {limit_for_scans}.")
    
    auth_params_for_connector: Dict[str, Any] = {}

    if args.command == "github":
        github_url_for_scan = None
        targets_to_scan = []
        target_entity_name = "Public GitHub.com organizations"

        if not args.gh_tk:
            main_logger.error("--gh-tk (GitHub token) is required for GitHub scans.")
            sys.exit(1)
        auth_params_for_connector["token"] = args.gh_tk
        if clients.github_connector.is_placeholder_token(args.gh_tk): # Check CLI token
            main_logger.error("GitHub token provided via --gh-tk is a placeholder. Cannot scan GitHub.")
            sys.exit(1)

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
            if not scan_and_process_single_target("github", target, cfg, privateid_manager, exemption_manager, global_repo_scan_counter, limit_for_scans, auth_params=auth_params_for_connector, platform_url=github_url_for_scan):
                overall_command_success = False 
            if limit_for_scans is not None and global_repo_scan_counter[0] >= limit_for_scans:
                 main_logger.warning(f"Global debug limit ({limit_for_scans}) reached. Stopping further GitHub target scans.")
                 break 
        main_logger.info("--- GitHub Scan Command Finished ---")
    
    elif args.command == "gitlab":
        if not args.gl_tk:
            main_logger.error("--gl-tk (GitLab token) is required for GitLab scans.")
            sys.exit(1)
        auth_params_for_connector["token"] = args.gl_tk
        if clients.gitlab_connector.is_placeholder_token(args.gl_tk): # Check CLI token
            main_logger.error("GitLab token provided via --gl-tk is a placeholder. Cannot scan GitLab.")
            sys.exit(1)

        targets_to_scan = get_targets_from_cli_or_env(args.groups, cfg.GITLAB_GROUPS_ENV, "GitLab groups", main_logger)
        if not targets_to_scan: sys.exit(0)

        gitlab_url_for_scan = args.gitlab_url if args.gitlab_url else cfg.GITLAB_URL_ENV
        if args.gitlab_url:
            main_logger.info(f"CLI override: Using GitLab URL: {args.gitlab_url}")

        main_logger.info(f"--- Starting GitLab Scan for {len(targets_to_scan)} Groups on {gitlab_url_for_scan} ---")
        for target in targets_to_scan:
            if not scan_and_process_single_target("gitlab", target, cfg, privateid_manager, exemption_manager, global_repo_scan_counter, limit_for_scans, auth_params=auth_params_for_connector, platform_url=gitlab_url_for_scan):
                overall_command_success = False 
            if limit_for_scans is not None and global_repo_scan_counter[0] >= limit_for_scans:
                 main_logger.warning(f"Global debug limit ({limit_for_scans}) reached. Stopping further GitLab target scans.")
                 break 
        main_logger.info("--- GitLab Scan Command Finished ---")

    elif args.command == "azure":
        # Determine Azure auth method
        if args.az_cid and args.az_cs and args.az_tid:
            main_logger.info("Using Azure Service Principal credentials from CLI.")
            auth_params_for_connector = {
                "spn_client_id": args.az_cid,
                "spn_client_secret": args.az_cs,
                "spn_tenant_id": args.az_tid
            }
            # Check for placeholders in SPN details
            if clients.azure_devops_connector.are_spn_details_placeholders(args.az_cid, args.az_cs, args.az_tid):
                main_logger.error("One or more Azure Service Principal CLI arguments are placeholders. Cannot scan Azure DevOps.")
                sys.exit(1)
        elif args.az_tk:
            main_logger.info("Using Azure PAT from CLI.")
            auth_params_for_connector = {"pat_token": args.az_tk}
            if clients.azure_devops_connector.is_placeholder_token(args.az_tk): # Check CLI token
                main_logger.error("Azure PAT provided via --az-tk is a placeholder. Cannot scan Azure DevOps.")
                sys.exit(1)
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
            if not scan_and_process_single_target("azure", target, cfg, privateid_manager, exemption_manager, global_repo_scan_counter, limit_for_scans, auth_params=auth_params_for_connector):
                overall_command_success = False 
            if limit_for_scans is not None and global_repo_scan_counter[0] >= limit_for_scans:
                 main_logger.warning(f"Global debug limit ({limit_for_scans}) reached. Stopping further Azure DevOps target scans.")
                 break 
        main_logger.info("--- Azure DevOps Scan Command Finished ---")

    elif args.command == "merge":
        backup_existing_file(cfg.OUTPUT_DIR, cfg.EXEMPTION_LOG_FILENAME)
        backup_existing_file(cfg.OUTPUT_DIR, cfg.PRIVATE_ID_FILENAME)
        if not merge_intermediate_catalogs(cfg, main_logger):
            overall_command_success = False 
        main_logger.info("--- Merge Command Finished ---")

    try:
        main_logger.info("Saving Exemption logs and Private ID mappings...")
        exemption_manager.save_all_exemptions() 
        privateid_manager.save_all_mappings()
        main_logger.info(f"Exemptions logged to: {cfg.EXEMPTION_LOG_FILEPATH}")
        main_logger.info(f"Private ID mappings saved to: {cfg.PRIVATE_ID_FILEPATH}")
    except Exception as save_err:
        main_logger.error(f"Error saving manager data: {save_err}", exc_info=True)
        overall_command_success = False 

    if overall_command_success:
        main_logger.info(f"Command '{args.command}' completed successfully.")
        sys.exit(0) 
    else:
        main_logger.error(f"Command '{args.command}' encountered errors. Please check logs.")
        sys.exit(1) 

if __name__ == "__main__":
    main_cli()
