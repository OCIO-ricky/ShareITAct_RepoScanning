# generate_codejson.py
"""
Main script for the Share IT Act Repository Scanning Tool.

This script orchestrates the process of scanning repositories across multiple
platforms (GitHub, GitLab, Azure DevOps), processing the collected metadata,
and generating a `code.json` file compliant with the code.gov schema v2.0.
Check the README for more details on how to set up the environment and run the
script.
"""

import os
import json
import logging
import logging.handlers
import time
import re
import argparse
import sys
import threading # For processed_counter_lock
import glob
from datetime import datetime, timezone # timedelta moved to script_utils
from typing import List, Optional, Dict, Any # Keep this line
# Changed to import setup_global_logging from logging_config
from utils.logging_config import setup_global_logging 
setup_global_logging()

# Set the logging level for gql.transport.requests to WARNING to suppress INFO logs
gql_transport_logger = logging.getLogger("gql.transport.requests")
gql_transport_logger.setLevel(logging.WARNING)

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
    from utils import Config, ExemptionLogger, RepoIdMappingManager
    from utils.script_utils import (
        setup_target_logger,
        write_json_file, backup_existing_file, backup_file_and_leave_original,
        parse_semver, infer_version, infer_status,
        process_and_finalize_repo_data_list,
        get_targets_from_cli_or_env, parse_azure_targets_from_string_list,
        format_duration
    )
except ImportError as e:
    print(f"Error importing utility modules: {e}")
    print("Please ensure 'utils' directory exists and contains necessary files like config.py, script_utils.py etc.")
    sys.exit(1)


# --- Constants ---
# Constants like INACTIVITY_THRESHOLD_YEARS, VALID_README_STATUSES, LOG_DIR_NAME
# have been moved to utils/script_utils.py
INTERMEDIATE_FILE_PATTERN = "intermediate_*.json"
CODE_JSON_SCHEMA_VERSION = "2.0"
CODE_JSON_MEASUREMENT_TYPE = {"method": "projects"}

# ANSI escape codes for coloring output (if not already defined globally)
ANSI_RED = "\x1b[31;1m"  # Bold Red
ANSI_RESET = "\x1b[0m"   # Reset to default color
ANSI_YELLOW = "\x1b[33;1m"  # Bold Yellow

# --- Core Scanning and Merging Functions ---
def scan_and_process_single_target(
    platform: str, 
    target_identifier: str, 
    cfg: Config, 
    repo_id_mapping_mgr: RepoIdMappingManager, # Updated parameter name
    exemption_mgr: ExemptionLogger, 
    processed_counter_lock: threading.Lock, # Added lock for the counter
    global_repo_counter: List[int], 
    limit_to_pass: Optional[int], 
    auth_params: Dict[str, Any], 
    platform_url: Optional[str] = None,
    hours_per_commit: Optional[float] = None 
) -> bool:
    """
    This function orchestrates the scanning of a specific target (like a GitHub organization or GitLab group) on a given platform.
    It calls the appropriate platform connector to fetch repository data, potentially using a previous scan's intermediate file for
    caching. After fetching, it finalizes the repository data (handling private IDs, exemptions, URL updates, etc.) and then writes
    the processed information to a new intermediate JSON file for that specific target. It also manages target-specific logging and 
    error handling for the scan of that single target.
    NOTE: For subsequent scans, the script uses the generated intermediate file from the previous run for a given target as a cache. 
          If a repository within that target hasn't changed (determined by its commit SHA), its data is loaded from this cached file, 
          significantly speeding up future runs by avoiding redundant API calls and data processing.
    """
    main_stream_logger = logging.getLogger(__name__) # Logger for messages intended for the main stdout stream

    # Log to main stream immediately that we are attempting to process this target
    main_stream_logger.info(f"Attempting to process {platform} target: {target_identifier}")

    target_logger_name = f"{platform}.{target_identifier.replace('/', '_').replace('.', '_')}"
    target_log_filename = f"{platform}_{target_identifier.replace('/', '_').replace('.', '_')}.log"
    target_logger = setup_target_logger(target_logger_name, target_log_filename, cfg.OUTPUT_DIR)

    # --- Determine the path for the previous scan's intermediate file for this specific target ---
    # This will be used as the cache input for the current scan of this target.
    previous_intermediate_filename = f"intermediate_{platform}_{target_identifier.replace('/', '_').replace('.', '_')}.json"
    previous_intermediate_filepath = os.path.join(cfg.OUTPUT_DIR, previous_intermediate_filename)

    try:
        target_logger_name = f"{platform}.{target_identifier.replace('/', '_').replace('.', '_')}"
        target_log_filename = f"{platform}_{target_identifier.replace('/', '_').replace('.', '_')}.log"
        target_logger = setup_target_logger(target_logger_name, target_log_filename, cfg.OUTPUT_DIR)
    except Exception as e_setup_log:
        main_stream_logger.error(f"Failed to setup target-specific logger for {platform} target {target_identifier}: {e_setup_log}. Processing cannot continue for this target.")
        return False # Indicate failure for this target

    # Now that target_logger is set up (or we've exited), log the more detailed start to it.
    target_logger.info(f"--- Starting scan for {platform} target: {target_identifier} ---", extra={'org_group': target_identifier})    
    if hours_per_commit is None or hours_per_commit == 0: hours_per_commit = None

    fetched_repos = []
    connector_success = False 

    if limit_to_pass is not None and global_repo_counter[0] >= limit_to_pass:        
        main_stream_logger.warning(f"Skipping {platform} target {target_identifier} due to global repository limit ({limit_to_pass}).")
        target_logger.warning(f"Global debug limit ({limit_to_pass}) reached. Skipping scan for {target_identifier}.", extra={'org_group': target_identifier})
        target_logger.info(f"--- Finished scan for {platform} target: {target_identifier} (Skipped due to limit) ---", extra={'org_group': target_identifier})
        return True 

    try:
        if platform == "github":
            fetched_repos = clients.github_connector.fetch_repositories(
                token=auth_params.get("token"), 
                org_name=target_identifier, 
                processed_counter=global_repo_counter, 
                processed_counter_lock=processed_counter_lock,
                debug_limit=limit_to_pass, 
                github_instance_url=platform_url,
                hours_per_commit=hours_per_commit,
                max_workers=cfg.SCANNER_MAX_WORKERS_ENV,
                cfg_obj=cfg, # Pass the cfg object
                previous_scan_output_file=previous_intermediate_filepath # Pass path to previous intermediate file
            )
            connector_success = True
        elif platform == "gitlab":
            fetched_repos = clients.gitlab_connector.fetch_repositories(
                token=auth_params.get("token"), 
                group_path=target_identifier, 
                processed_counter=global_repo_counter, 
                processed_counter_lock=processed_counter_lock, 
                debug_limit=limit_to_pass, 
                gitlab_instance_url=platform_url, 
                hours_per_commit=hours_per_commit,
                max_workers=cfg.SCANNER_MAX_WORKERS_ENV,
                cfg_obj=cfg, # Pass the cfg object
                previous_scan_output_file=previous_intermediate_filepath # Pass path to previous intermediate file
            )
            connector_success = True
        elif platform == "azure":
            if '/' not in target_identifier:
                 target_logger.error(f"Invalid Azure DevOps target format: '{target_identifier}'. Expected Org/Project.")
                 main_stream_logger.error(f"Invalid Azure DevOps target format for {target_identifier}. Expected Org/Project. Skipping this target.")
                 return False
            # The ADO connector's fetch_repositories now takes target_path ("org/project")
            # and internally handles splitting if needed.
            fetched_repos = clients.azure_devops_connector.fetch_repositories(
                token=auth_params.get("pat_token"), # 'token' is for PAT
                target_path=target_identifier,      # Pass the full "org/project" path
                processed_counter=global_repo_counter,
                processed_counter_lock=processed_counter_lock,
                debug_limit=limit_to_pass, 
                azure_devops_url=platform_url, # This will be None if not set by CLI, connector handles default
                hours_per_commit=hours_per_commit,
                max_workers=cfg.SCANNER_MAX_WORKERS_ENV,
                cfg_obj=cfg, # Pass the cfg object
                previous_scan_output_file=previous_intermediate_filepath, # Pass cache file
                spn_client_id=auth_params.get("spn_client_id"), # Explicitly pass SPN details
                spn_client_secret=auth_params.get("spn_client_secret"),
                spn_tenant_id=auth_params.get("spn_tenant_id")
            )
            connector_success = True
        else:
            target_logger.error(f"Unknown platform: {platform}")
            main_stream_logger.error(f"Unknown platform: {platform} for target: {target_identifier}. Skipping this target.")
            return False
        
    except Exception as e:
        target_logger.critical(f"Critical error during {platform} connector execution for {target_identifier}: {e}", exc_info=True, extra={'org_group': target_identifier})
        main_stream_logger.error(f"Critical error during connector execution for {platform} target {target_identifier}. See target-specific log for details. Skipping this target.")
        target_logger.info(f"--- Finished scan for {platform} target: {target_identifier} (Critical Connector Error) ---", extra={'org_group': target_identifier})
        return False 

    if connector_success:
        if fetched_repos:
            target_logger.info(f"Connector returned {len(fetched_repos)} repositories for {target_identifier}.", extra={'org_group': target_identifier})
            main_stream_logger.info(f"Found {len(fetched_repos)} repositories for {platform} target: {target_identifier}.")
        else:
            target_logger.info(f"Connector returned no repositories for {target_identifier}.", extra={'org_group': target_identifier})
            main_stream_logger.info(f"No repositories found to process for {platform} target: {target_identifier}.")

    default_org_ids_for_exemption_processor = [target_identifier] 
    if platform == "azure" and '/' in target_identifier:
        default_org_ids_for_exemption_processor = [target_identifier.split('/',1)[0]]


    if not fetched_repos and connector_success :
        target_logger.info(f"{ANSI_YELLOW}No repositories to process for {target_identifier} or limit reached within connector.{ANSI_RESET}", extra={'org_group': target_identifier})
        # main_stream_logger already handled the "No repositories found" case above if connector_success was true.
        intermediate_data = [] 
    else:
        target_logger.info(f"Finalizing {len(fetched_repos)} repositories for {target_identifier}...", extra={'org_group': target_identifier})
        intermediate_data = process_and_finalize_repo_data_list(
            [repo for repo in fetched_repos if repo is not None and not repo.get("processing_error")], 
            cfg, repo_id_mapping_mgr, exemption_mgr, target_logger, platform # Pass platform
        )
        errored_repos = [repo for repo in fetched_repos if repo and repo.get("processing_error")]
        if errored_repos:
            intermediate_data.extend(errored_repos)


    intermediate_filename = f"intermediate_{platform}_{target_identifier.replace('/', '_').replace('.', '_')}.json"
    intermediate_filepath = os.path.join(cfg.OUTPUT_DIR, intermediate_filename)
     # Log the content for mynodejs specifically before writing to intermediate file
    for item_to_log in intermediate_data: # intermediate_data is a list of dicts
        if isinstance(item_to_log, dict) and item_to_log.get("name") == "mynodejs":
            repo_org_group = f"{item_to_log.get('organization', target_identifier)}/{item_to_log.get('name', 'UnknownRepo')}"
            target_logger.info(f"Repo: mynodejs - Permissions content for intermediate file: {item_to_log.get('permissions')}", extra={'org_group': repo_org_group})
            break
   
    if write_json_file(intermediate_data, intermediate_filepath):
        target_logger.info(f"Successfully wrote intermediate data to {intermediate_filepath}", extra={'org_group': target_identifier})
        target_logger.info(f"--- Finished scan for {platform} target: {target_identifier} ---", extra={'org_group': target_identifier})
        return True 
    else:
        target_logger.error(f"Failed to write intermediate data for {target_identifier}.", extra={'org_group': target_identifier})
        target_logger.info(f"--- Finished scan for {platform} target: {target_identifier} (Write Error) ---", extra={'org_group': target_identifier})
        return False 

def merge_intermediate_catalogs(cfg: Config, main_logger: logging.Logger) -> bool: # Removed repo_id_mapping_mgrNOTE
    """
    Merges intermediate catalog files into a single catalog. 
    Args:
        cfg (Config): Configuration object.
        main_logger (logging.Logger): Logger for main operations.
        repo_id_mapping_mgr (RepoIdMappingManager): Manager for repository ID mappings. (privateid's)
        returns:
        bool: True if merge operation is successful, False otherwise.
    """
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
        project_org_group = f"{project_data.get('organization', 'UnknownOrg')}/{project_data.get('name', 'UnknownRepo')}"
        if "processing_error" in project_data and len(project_data.keys()) <= 3: # e.g. name, org, error
            main_logger.warning(f"Skipping project entry during merge due to processing_error: {project_data.get('name', 'Unknown')}", extra={'org_group': project_org_group})
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
            main_logger.debug(f"Repo {updated_project_data.get('name')}: Standardized visibility from 'internal' to 'private' for final output.", extra={'org_group': project_org_group})
        
        # The 'privateID' field (now containing prefixed repo_id) comes from the intermediate file.
        # No need to generate or modify it here.
        main_logger.debug(f"Repo {updated_project_data.get('name')}: Using privateID '{updated_project_data.get('privateID')}' from intermediate file.", extra={'org_group': project_org_group})

       # Remove _is_empty_repo and lastCommitSHA if they exist
        updated_project_data.pop('_private_contact_emails', None)
        updated_project_data.pop("_is_empty_repo", None)
        updated_project_data.pop("lastCommitSHA", None)
        updated_project_data.pop("repo_id", None)
        
        processed_projects_for_final_catalog.append(updated_project_data)

    backup_existing_file(cfg.OUTPUT_DIR, cfg.CATALOG_JSON_FILE)
    
    final_code_json_structure["projects"] = processed_projects_for_final_catalog
    final_code_json_structure["projects"].sort(key=lambda x: x.get("name", "").lower()) # Sort projects by name

    final_catalog_filepath = os.path.join(cfg.OUTPUT_DIR, cfg.CATALOG_JSON_FILE)
    if write_json_file(final_code_json_structure, final_catalog_filepath):
        main_logger.info(f"Successfully merged {len(processed_projects_for_final_catalog)} projects into {final_catalog_filepath}")
        main_logger.info("--- Merge Operation Finished Successfully ---")

        # --- Calculate and Log Final Statistics ---
        stats = {
            "total_projects_in_catalog": 0,
            "public_projects_count": 0,
            "private_projects_count": 0, # Includes 'internal' after normalization
            "exempted_projects_count": 0,
            "exemptions_by_type_overall": {}, # e.g., {"exemptByLaw": 10, "exemptNonCode": 5}
            "exempted_count_by_organization": {} # e.g., {"OrgA": 5, "OrgB": 2}
        }

        for project_data in final_code_json_structure["projects"]:
            if "processing_error" in project_data and len(project_data.keys()) <= 3:
                # This was an error placeholder, not a real project entry for catalog stats
                continue

            stats["total_projects_in_catalog"] += 1
            org_name = project_data.get("organization", "UnknownOrganization")
            # Visibility here is after normalization in the loop above
            visibility = project_data.get("repositoryVisibility", "").lower()
            usage_type = project_data.get("permissions", {}).get("usageType", "")

            if visibility == "public":
                stats["public_projects_count"] += 1
            elif visibility == "private": # 'internal' has been normalized to 'private'
                stats["private_projects_count"] += 1

            if usage_type and usage_type.lower().startswith("exempt"):
                stats["exempted_projects_count"] += 1
                stats["exemptions_by_type_overall"][usage_type] = stats["exemptions_by_type_overall"].get(usage_type, 0) + 1
                stats["exempted_count_by_organization"][org_name] = stats["exempted_count_by_organization"].get(org_name, 0) + 1
        
        main_logger.info("--- Final Catalog Statistics ---")
        main_logger.info(f"Total Projects in Catalog: {stats['total_projects_in_catalog']}")
        main_logger.info(f"  Public Projects: {stats['public_projects_count']}")
        main_logger.info(f"  Private Projects (incl. internal): {stats['private_projects_count']}")
        main_logger.info(f"Total Exempted Projects: {stats['exempted_projects_count']}")
        if stats["exempted_projects_count"] > 0:
            main_logger.info("  Exemptions by Type (Overall):")
            for ex_type, count in sorted(stats["exemptions_by_type_overall"].items()):
                main_logger.info(f"    - {ex_type}: {count}")
            main_logger.info("  Exempted Count by Organization:")
            for org, count in sorted(stats["exempted_count_by_organization"].items()):
                main_logger.info(f"    - {org}: {count}")
        # --- End Final Statistics ---
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
def _get_and_validate_token(
    cli_token_value: Optional[str], 
    env_var_name: str, 
    cli_arg_name: str, 
    platform_name: str, 
    placeholder_check_func: callable, 
    main_logger: logging.Logger
) -> str:
    token_to_use = None
    source = ""
    if cli_token_value:
        token_to_use = cli_token_value
        source = f"--{cli_arg_name} CLI argument"
    elif os.getenv(env_var_name):
        token_to_use = os.getenv(env_var_name)
        source = f"{env_var_name} environment variable"
    if not token_to_use:
        main_logger.error(f"{platform_name} PAT not found. Please provide it via --{cli_arg_name} or the {env_var_name} environment variable.")
        sys.exit(1)
    if placeholder_check_func(token_to_use):
        main_logger.error(f"{platform_name} PAT provided via {source} is a placeholder. Cannot scan {platform_name}.")
        sys.exit(1)
    main_logger.info(f"Using {platform_name} token from {source}.")
    return token_to_use

# --- Main CLI Function ---
def main_cli(): 
    script_start_time = time.time() # Record the start time of the script

    # --- BEGIN PRE-CONFIG DIAGNOSTIC ---
    # This attempts to load .env directly to see what os.getenv() reports.
    # This is for diagnosis if the Config class seems to get a truncated list.
    try:
        from dotenv import load_dotenv as load_dotenv_direct # Temporary import for direct test
        import os as os_direct # Temporary import for direct test
        
        # Assuming .env is in the current working directory when generate_codejson.py is run
        env_path_direct = os_direct.path.join(os_direct.getcwd(), '.env')
        # Create a temporary logger for this pre-config diagnostic
        pre_config_diag_logger = logging.getLogger("pre_config_diag")
        pre_config_diag_logger.info(f"DIAGNOSTIC_PRE_CONFIG: Attempting to load .env from: {env_path_direct}")
        found_dotenv_direct = load_dotenv_direct(dotenv_path=env_path_direct, override=False) # override=False is important
        pre_config_diag_logger.info(f"DIAGNOSTIC_PRE_CONFIG: load_dotenv_direct found .env: {found_dotenv_direct}")
        raw_github_orgs_direct = os_direct.getenv("GITHUB_ORGS")
        pre_config_diag_logger.info(f"DIAGNOSTIC_PRE_CONFIG: Raw GITHUB_ORGS from os.getenv after explicit load_dotenv_direct: '{raw_github_orgs_direct}'")
    except Exception as e_pre_diag:
        logging.getLogger("pre_config_diag").error(f"Error in PRE-CONFIG DIAGNOSTIC block: {e_pre_diag}")
    # --- END PRE-CONFIG DIAGNOSTIC ---

    cfg = Config()
    setup_global_logging()
    # --- BEGIN DIAGNOSTIC LOGGING ---
    main_logger_diag = logging.getLogger(__name__) 
    main_logger_diag.info(f"DIAGNOSTIC: Initial GITHUB_ORGS_ENV from cfg.GITHUB_ORGS_ENV: {cfg.GITHUB_ORGS_ENV} (type: {type(cfg.GITHUB_ORGS_ENV)})")
    main_logger_diag.info(f"DIAGNOSTIC: Initial DEBUG_REPO_LIMIT from cfg.DEBUG_REPO_LIMIT: {cfg.DEBUG_REPO_LIMIT} (type: {type(cfg.DEBUG_REPO_LIMIT)})")
    # --- END DIAGNOSTIC LOGGING ---
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
    global_repo_scan_counter_lock = threading.Lock() # Create the lock here

    # Determine repository processing limit (CLI > .env > no limit)
    limit_for_scans = None
    cli_limit_val = getattr(args, 'limit', None)
    if cli_limit_val is not None:
        if cli_limit_val > 0:
            limit_for_scans = cli_limit_val
            main_logger.info(f"CLI override: Repository processing limit set to {limit_for_scans} for this run.")
        else: # CLI limit is 0 or less, meaning no limit
            main_logger.info(f"CLI override: --limit set to {cli_limit_val}, effectively no limit for this run (processing all).")
            limit_for_scans = None # Explicitly set to None for "no limit"
    elif cfg.DEBUG_REPO_LIMIT is not None:
        if cfg.DEBUG_REPO_LIMIT > 0: # Only apply .env limit if it's greater than 0
            limit_for_scans = cfg.DEBUG_REPO_LIMIT
            main_logger.info(f"Using repository processing limit from .env: {limit_for_scans}.")
        else: # .env limit is 0 or less, meaning no limit
            main_logger.info(f"Repository processing limit from .env is {cfg.DEBUG_REPO_LIMIT}, effectively no limit for this run (processing all).")
            limit_for_scans = None # Explicitly set to None for "no limit"
    else: # No CLI limit, and DEBUG_REPO_LIMIT is None (e.g., key not in .env or value was empty)
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
    
    if hours_per_commit_for_scan is not None and hours_per_commit_for_scan > 0:
        source_msg = "CLI" if cli_hpc_val is not None else ".env"
        main_logger.info(f"Labor hours estimation ENABLED. Hours per commit set to {hours_per_commit_for_scan} (Source: {source_msg}).")
    else: 
        main_logger.info(f"{ANSI_RED}Labor hours estimation DISABLED for this run (no valid hours_per_commit from CLI or .env).{ANSI_RESET}")
    auth_params_for_connector: Dict[str, Any] = {}

    if args.command == "github":
        github_url_for_scan = None
        targets_to_scan = []
        target_entity_name = "Public GitHub.com organizations"
        
        # Get GitHub token: CLI > ENV
        auth_params_for_connector["token"] = _get_and_validate_token(
            cli_token_value=args.gh_tk,
            env_var_name="GITHUB_TOKEN",
            cli_arg_name="gh-tk",
            platform_name="GitHub",
            placeholder_check_func=clients.github_connector.is_placeholder_token,
            main_logger=main_logger
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
            targets_to_scan = get_targets_from_cli_or_env(args.orgs, cfg.GITHUB_ORGS_ENV, "GitHub.com organizations", main_logger)
            main_logger.info("Targeting public GitHub.com")

        # --- BEGIN DIAGNOSTIC LOGGING ---
        main_logger_diag.info(f"DIAGNOSTIC: Targets to scan for GitHub: {targets_to_scan}")
        main_logger_diag.info(f"DIAGNOSTIC: Length of targets_to_scan: {len(targets_to_scan) if targets_to_scan else 0}")
        main_logger_diag.info(f"DIAGNOSTIC: limit_for_scans before loop: {limit_for_scans} (type: {type(limit_for_scans)})")
        # --- END DIAGNOSTIC LOGGING ---
        if not targets_to_scan: 
            main_logger.info(f"No {target_entity_name} specified to scan.")
            sys.exit(0)
        
        main_logger.info(f"--- Starting GitHub Scan for {len(targets_to_scan)} {target_entity_name} ---")
        for target in targets_to_scan:
            try:
                success_for_target = scan_and_process_single_target(
                    "github", target, cfg, repo_id_mapping_manager, exemption_manager, 
                    global_repo_scan_counter_lock, global_repo_scan_counter, limit_for_scans, 
                    auth_params_for_connector, platform_url=github_url_for_scan, 
                    hours_per_commit=hours_per_commit_for_scan
                )
                if not success_for_target:
                    overall_command_success = False 
                    # Specific error should have been logged by scan_and_process_single_target or its callees to main_stream_logger
                    main_logger.warning(f"Processing marked as unsuccessful for GitHub target: {target}. Check logs for details.")
            except Exception as e_target_processing:
                main_logger.critical(
                    f"Unhandled exception during processing of GitHub target: {target}. Error: {e_target_processing}. Skipping to next target.",
                    exc_info=True
                )
                overall_command_success = False

            if limit_for_scans is not None and global_repo_scan_counter[0] >= limit_for_scans:
                 main_logger.warning(f"Global repository limit ({limit_for_scans}) reached. Stopping further GitHub target scans.")
                 break 
        main_logger.info("--- GitHub Scan Command Finished ---")
    
    elif args.command == "gitlab":
        # Get GitLab token: CLI > ENV
        auth_params_for_connector["token"] = _get_and_validate_token(
            cli_token_value=args.gl_tk,
            env_var_name="GITLAB_TOKEN",
            cli_arg_name="gl-tk",
            platform_name="GitLab",
            placeholder_check_func=clients.gitlab_connector.is_placeholder_token,
            main_logger=main_logger
        )

        targets_to_scan = get_targets_from_cli_or_env(args.groups, cfg.GITLAB_GROUPS_ENV, "GitLab groups", main_logger)
        if not targets_to_scan: sys.exit(0)

        gitlab_url_for_scan = args.gitlab_url if args.gitlab_url else cfg.GITLAB_URL_ENV
        if args.gitlab_url:
            main_logger.info(f"CLI override: Using GitLab URL: {args.gitlab_url}")

        main_logger.info(f"--- Starting GitLab Scan for {len(targets_to_scan)} Groups on {gitlab_url_for_scan} ---")
        for target in targets_to_scan:
            try:
                success_for_target = scan_and_process_single_target(
                    "gitlab", target, cfg, repo_id_mapping_manager, exemption_manager, 
                    global_repo_scan_counter_lock, global_repo_scan_counter, limit_for_scans, 
                    auth_params_for_connector, platform_url=gitlab_url_for_scan, 
                    hours_per_commit=hours_per_commit_for_scan
                )
                if not success_for_target:
                    overall_command_success = False
                    main_logger.warning(f"Processing marked as unsuccessful for GitLab target: {target}. Check logs for details.")
            except Exception as e_target_processing:
                main_logger.critical(
                    f"Unhandled exception during processing of GitLab target: {target}. Error: {e_target_processing}. Skipping to next target.",
                    exc_info=True
                )
                overall_command_success = False
            if limit_for_scans is not None and global_repo_scan_counter[0] >= limit_for_scans:
                 main_logger.warning(f"Global repository limit ({limit_for_scans}) reached. Stopping further GitLab target scans.")
                 break 
        main_logger.info("--- GitLab Scan Command Finished ---")

    elif args.command == "azure":
        # Azure DevOps can use PAT or Service Principal
        # Prioritize SPN from CLI if all parts are present, then PAT from CLI, then fallbacks to ENV for each.
        
        # Check for SPN details from CLI first
        spn_cid_cli = getattr(args, 'az_cid', None)
        spn_cs_cli = getattr(args, 'az_cs', None)
        spn_tid_cli = getattr(args, 'az_tid', None)

        if spn_cid_cli and spn_cs_cli and spn_tid_cli:
            if clients.azure_devops_connector.are_spn_details_placeholders(spn_cid_cli, spn_cs_cli, spn_tid_cli):
                main_logger.error("One or more Azure Service Principal CLI arguments are placeholders. Cannot scan Azure DevOps with SPN from CLI.")
                sys.exit(1)
            main_logger.info("Using Azure Service Principal credentials from CLI arguments.")
            auth_params_for_connector = {
                "spn_client_id": spn_cid_cli,
                "spn_client_secret": spn_cs_cli,
                "spn_tenant_id": spn_tid_cli
            }
        else: # Fallback to PAT (CLI then ENV) or SPN from ENV
            pat_token_from_cli = getattr(args, 'az_tk', None)
            auth_params_for_connector["pat_token"] = _get_and_validate_token(
                cli_token_value=pat_token_from_cli,
                env_var_name="AZURE_DEVOPS_TOKEN",
                cli_arg_name="az-tk",
                platform_name="Azure DevOps",
                placeholder_check_func=clients.azure_devops_connector.is_placeholder_token,
                main_logger=main_logger
            )
            # Note: If SPN details are also in .env, the connector logic will prioritize SPN if all parts are valid there.

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
            try:
                success_for_target = scan_and_process_single_target(
                    "azure", target, cfg, repo_id_mapping_manager, exemption_manager, 
                    global_repo_scan_counter_lock, global_repo_scan_counter, limit_for_scans, 
                    auth_params_for_connector, platform_url=None, # Azure connector handles its own URL via config or default
                    hours_per_commit=hours_per_commit_for_scan
                )
                if not success_for_target:
                    overall_command_success = False
                    main_logger.warning(f"Processing marked as unsuccessful for Azure DevOps target: {target}. Check logs for details.")
            except Exception as e_target_processing:
                main_logger.critical(
                    f"Unhandled exception during processing of Azure DevOps target: {target}. Error: {e_target_processing}. Skipping to next target.",
                    exc_info=True
                )
                overall_command_success = False
            if limit_for_scans is not None and global_repo_scan_counter[0] >= limit_for_scans:
                 main_logger.warning(f"Global repository limit ({limit_for_scans}) reached. Stopping further Azure DevOps target scans.")
                 break 
        main_logger.info("--- Azure DevOps Scan Command Finished ---")

    elif args.command == "merge":
        backup_file_and_leave_original(cfg.OUTPUT_DIR, cfg.EXEMPTION_LOG_FILENAME)
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
    formatted_duration = format_duration(total_duration_seconds)
    main_logger.info(f"Total script execution time: {formatted_duration}.")

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
