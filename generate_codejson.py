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
from datetime import datetime, timezone 
from typing import List, Optional, Dict, Any, Callable # Keep this line
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
    from utils.script_utils import ( # Keep this line
        setup_target_logger,
        write_json_file, backup_existing_file,
        parse_semver, infer_version, infer_status,
        process_and_finalize_repo_data_list, # Removed backup_and_clear_log_file
        get_targets_from_cli_or_env, parse_azure_targets_from_string_list,
        format_duration
    )
    from utils.caching import load_previous_scan_data # Added import for cache loading
except ImportError as e:
    # This print is fine for critical startup errors
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
ANSI_GREEN = "\x1b[32;1m"  # Bold Green

# --- Helper for Platform Scan Orchestration ---
def _orchestrate_platform_scan(
    platform_name: str,
    args: argparse.Namespace,
    cfg: Config,
    repo_id_mapping_mgr: RepoIdMappingManager,
    exemption_mgr: ExemptionLogger,
    global_repo_scan_counter: List[int],
    global_repo_scan_counter_lock: threading.Lock,
    limit_for_scans: Optional[int],
    hours_per_commit_for_scan: Optional[float],
    main_logger: logging.Logger,
    # Platform-specific functions and attributes
    auth_setup_func: Callable,
    target_retrieval_func: Callable,
    estimate_calls_func: Callable,
    get_rate_limit_status_func: Callable,
    calculate_inter_submission_delay_func: Callable,
    connector_module: Any, # The connector module itself
    cli_target_arg_name: str,
    env_target_cfg_attr_name: str,
    entity_name_plural: str,
    total_estimated_calls_cfg_attr: str,
    target_parsing_func: Optional[Callable] = None,
    cli_token_arg_name: Optional[str] = None,
    env_token_var_name: Optional[str] = None,
    platform_url_cli_arg_name: Optional[str] = None,
    platform_url_cfg_attr_name: Optional[str] = None,
    placeholder_check_func: Optional[Callable] = None,
    # Specific for GitHub GQL client sharing
    requires_common_gql_client: bool = False
) -> bool:
    """Orchestrates the pre-scan, rate limit setup, and main scan for a given platform."""
    overall_platform_success = True

    # 1. Authentication
    auth_params: Dict[str, Any] = {}
    if platform_name == "azure":
        auth_params = auth_setup_func(args, cfg, main_logger)
    elif cli_token_arg_name and env_token_var_name and placeholder_check_func:
        token = auth_setup_func(
            getattr(args, cli_token_arg_name, None),
            env_token_var_name,
            cli_token_arg_name,
            platform_name,
            placeholder_check_func,
            main_logger
        )
        auth_params["token"] = token
    else:
        main_logger.error(f"Auth setup misconfigured for platform {platform_name}")
        return False

    # 2. Platform URL
    platform_url_for_scan = getattr(args, platform_url_cli_arg_name, None) if platform_url_cli_arg_name else None
    if not platform_url_for_scan and platform_url_cfg_attr_name:
        platform_url_for_scan = getattr(cfg, platform_url_cfg_attr_name, None)
    if platform_name == "gitlab" and not platform_url_for_scan: # GitLab needs a default if none provided
        platform_url_for_scan = cfg.GITLAB_URL_ENV


    # 3. Target Retrieval
    raw_targets_list = target_retrieval_func(
        getattr(args, cli_target_arg_name, None),
        getattr(cfg, env_target_cfg_attr_name),
        entity_name_plural,
        main_logger
    )

    targets_to_scan: List[str] = []
    if target_parsing_func: # For Azure
        targets_to_scan = target_parsing_func(raw_targets_list, cfg.AZURE_DEVOPS_ORG_ENV, main_logger)
    else:
        targets_to_scan = raw_targets_list

    if not targets_to_scan:
        main_logger.info(f"No {entity_name_plural} specified to scan for {platform_name}.")
        return True # Not a failure, just nothing to do

    # 4. Pre-scan for API Call Estimation
    platform_total_estimated_api_calls = 0
    prescan_data_map: Dict[str, Dict[str, Any]] = {}
    main_logger.info(f"--- Starting {platform_name} Pre-scan for API Call Estimation for {len(targets_to_scan)} {entity_name_plural} ---")

    client_for_rate_limit = None # Initialize
    if platform_name == "github" and targets_to_scan:
        client_for_rate_limit, _, _, _ = connector_module._initialize_clients_for_org(auth_params.get("token"), targets_to_scan[0], platform_url_for_scan, main_logger)
    elif platform_name == "gitlab" and targets_to_scan:
        client_for_rate_limit, _ = connector_module._initialize_gitlab_client_and_get_group(
            connector_module._get_effective_gitlab_url(platform_url_for_scan, cfg, main_logger),
            auth_params.get("token"), targets_to_scan[0],
            os.getenv("DISABLE_SSL_VERIFICATION", "false").lower() != "true", main_logger
        )
    elif platform_name == "azure" and targets_to_scan:
        try:
            temp_creds, _ = connector_module._setup_azure_devops_credentials(
                auth_params.get("pat_token"), auth_params.get("spn_client_id"),
                auth_params.get("spn_client_secret"), auth_params.get("spn_tenant_id"), main_logger
            )
            if temp_creds and connector_module.AZURE_SDK_AVAILABLE:
                first_org = targets_to_scan[0].split('/')[0]
                effective_ado_url = platform_url_for_scan if platform_url_for_scan else cfg.AZURE_DEVOPS_API_URL_ENV
                org_url = f"{effective_ado_url.strip('/')}/{first_org}"
                client_for_rate_limit = connector_module.Connection(base_url=org_url, creds=temp_creds)
        except Exception as e_conn_rate:
            main_logger.warning(f"Could not establish {platform_name} connection for rate limit check: {e_conn_rate}")

    for target_id in targets_to_scan:
        try:
            if platform_name == "github":
                enriched_list, estimated_calls = estimate_calls_func(token=auth_params.get("token"), org_name=target_id, github_instance_url=platform_url_for_scan, cfg_obj=cfg, logger_instance=main_logger)
            elif platform_name == "gitlab":
                enriched_list, estimated_calls = estimate_calls_func(token=auth_params.get("token"), group_path=target_id, gitlab_instance_url=platform_url_for_scan, cfg_obj=cfg, logger_instance=main_logger)
            elif platform_name == "azure":
                enriched_list, estimated_calls = estimate_calls_func(pat_token=auth_params.get("pat_token"), target_path=target_id, azure_devops_url=platform_url_for_scan, cfg_obj=cfg, logger_instance=main_logger, spn_client_id=auth_params.get("spn_client_id"), spn_client_secret=auth_params.get("spn_client_secret"), spn_tenant_id=auth_params.get("spn_tenant_id"))
            else: # Should not happen
                main_logger.error(f"Unknown platform {platform_name} for estimation.")
                continue

            if enriched_list is None:
                main_logger.error(f"Pre-scan for {platform_name} target '{target_id}' failed. Skipping this target for estimation.")
                continue
            prescan_data_map[target_id] = {"enriched_list": enriched_list, "estimate": estimated_calls}
            platform_total_estimated_api_calls += estimated_calls
            main_logger.info(f"Pre-scan for {platform_name} target '{target_id}': Found {len(enriched_list)} items, Estimated API calls: {estimated_calls}")
        except Exception as e_est:
            main_logger.error(f"Error during API call estimation for {platform_name} target '{target_id}': {e_est}", exc_info=True)

    main_logger.info(f"--- {platform_name} Pre-scan Finished. Total estimated API calls for all {platform_name} targets: {platform_total_estimated_api_calls} ---")
    setattr(cfg, total_estimated_calls_cfg_attr, platform_total_estimated_api_calls)

    # 5. Calculate Global Inter-Submission Delay
    global_platform_delay = 0.0
    if platform_total_estimated_api_calls > 0 and client_for_rate_limit:
        rate_status_args = [client_for_rate_limit, main_logger]
        if platform_name == "azure": # Azure's get_rate_limit_status needs org_name
            rate_status_args.insert(1, targets_to_scan[0].split('/')[0])

        platform_rate_status = get_rate_limit_status_func(*rate_status_args)
        global_platform_delay = calculate_inter_submission_delay_func(
            rate_limit_status=platform_rate_status,
            estimated_api_calls_for_target=platform_total_estimated_api_calls,
            num_workers=cfg.SCANNER_MAX_WORKERS_ENV,
            safety_factor=cfg.API_SAFETY_FACTOR_ENV,
            min_delay_seconds=cfg.MIN_INTER_REPO_DELAY_SECONDS_ENV,
            max_delay_seconds=cfg.MAX_INTER_REPO_DELAY_SECONDS_ENV
        )
        main_logger.info(f"Calculated GLOBAL inter-submission delay for {platform_name}: {global_platform_delay:.3f}s")
    elif platform_total_estimated_api_calls > 0:
        main_logger.warning(f"Could not get {platform_name} client for rate limit status to calculate global delay. Using default (max delay).")
        global_platform_delay = cfg.MAX_INTER_REPO_DELAY_SECONDS_ENV

    # 6. Main Scan Loop
    main_logger.info(f"--- Starting {platform_name} Scan for {len(targets_to_scan)} {entity_name_plural} ---")
    common_gql_client_for_workers, common_gql_endpoint_for_workers = None, None
    if requires_common_gql_client and targets_to_scan and platform_name == "github": # Specific to GitHub
         _, _, common_gql_client_for_workers, common_gql_endpoint_for_workers = connector_module._initialize_clients_for_org(
            auth_params.get("token"), targets_to_scan[0], platform_url_for_scan, main_logger
        )

    for target_id in targets_to_scan:
        try:
            prescan_info = prescan_data_map.get(target_id)
            if not prescan_info or "enriched_list" not in prescan_info:
                main_logger.error(f"Pre-scan data (enriched list) not found for {platform_name} target '{target_id}'. Skipping.")
                overall_platform_success = False
                continue

            success_for_target = scan_and_process_single_target(
                platform=platform_name,
                target_identifier=target_id,
                cfg=cfg,
                repo_id_mapping_mgr=repo_id_mapping_mgr,
                exemption_mgr=exemption_mgr,
                processed_counter_lock=global_repo_scan_counter_lock,
                global_repo_counter=global_repo_scan_counter,
                limit_to_pass=limit_for_scans,
                auth_params=auth_params,
                pre_fetched_enriched_repos=prescan_info["enriched_list"],
                global_inter_submission_delay=global_platform_delay,
                platform_url=platform_url_for_scan,
                hours_per_commit=hours_per_commit_for_scan,
                gql_client_for_workers=common_gql_client_for_workers if platform_name == "github" else None,
                graphql_endpoint_url_for_workers=common_gql_endpoint_for_workers if platform_name == "github" else None
            )
            if not success_for_target:
                overall_platform_success = False
                main_logger.warning(f"Processing marked as unsuccessful for {platform_name} target: {target_id}. Check logs for details.")
        except Exception as e_target_processing:
            main_logger.critical(f"Unhandled exception during processing of {platform_name} target: {target_id}. Error: {e_target_processing}. Skipping.", exc_info=True)
            overall_platform_success = False
        if limit_for_scans is not None and global_repo_scan_counter[0] >= limit_for_scans:
             main_logger.warning(f"Global repository limit ({limit_for_scans}) reached. Stopping further {platform_name} target scans.")
             break
    main_logger.info(f"--- {platform_name} Scan Command Finished ---")
    return overall_platform_success

# --- Core Scanning and Merging Functions ---
def scan_and_process_single_target(
    platform: str,
    target_identifier: str,
    cfg: Config,
    repo_id_mapping_mgr: RepoIdMappingManager, # (privateids , contact emails, etc)
    exemption_mgr: ExemptionLogger,
    processed_counter_lock: threading.Lock, # Added lock for the counter
    global_repo_counter: List[int],
    limit_to_pass: Optional[int],
    auth_params: Dict[str, Any], # Contains tokens, SPN details
    pre_fetched_enriched_repos: Optional[List[Dict[str, Any]]] = None, # New: Enriched list from pre-scan
    global_inter_submission_delay: Optional[float] = None, # New: Calculated global delay
    platform_url: Optional[str] = None,
    hours_per_commit: Optional[float] = None,
    # Platform-specific client/endpoint info for GitHub workers
    gql_client_for_workers: Optional[Any] = None,
    graphql_endpoint_url_for_workers: Optional[str] = None
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
    main_stream_logger.info(f"{ANSI_GREEN}Attempting to process {platform} target: {target_identifier}{ANSI_RESET}")

    # --- Log File Backup and Clearing Logic ---
    # Construct log file name and path components
    target_log_filename = f"{platform}_{target_identifier.replace('/', '_').replace('.', '_')}.log"
    log_dir_for_target = os.path.join(cfg.OUTPUT_DIR, "logs") # Assuming LOG_DIR_NAME is "logs"

    # Ensure the log directory exists
    os.makedirs(log_dir_for_target, exist_ok=True)

    # Backup the existing log file for this target (if it exists).
    # backup_existing_file renames the original, so the path will be free for a new log file.
    # Logging for this action is handled within backup_existing_file.
    # Use main_stream_logger for messages before target_logger is set up.
    main_stream_logger.info(f"Checking for existing log file '{os.path.join(log_dir_for_target, target_log_filename)}' to backup...")
    backup_existing_file(log_dir_for_target, target_log_filename)
    main_stream_logger.info(f"Log file backup process complete for '{target_log_filename}'. A new log file will be created if logging occurs for this target.")
    # --- End Log File Backup and Clearing Logic ---

    # --- Setup Target-Specific Logger ---
    # This must happen AFTER backup/clear and BEFORE first use of target_logger.
    target_logger_name = platform # e.g., "github", "gitlab", "azure"
    try:
        target_logger = setup_target_logger(target_logger_name, target_log_filename, cfg.OUTPUT_DIR)
    except Exception as e_setup_log:
        main_stream_logger.error(f"Failed to setup target-specific logger for {platform} target {target_identifier}: {e_setup_log}. Processing cannot continue for this target.")
        return False # Indicate failure for this target
    # --- End Setup Target-Specific Logger ---

    # Create the LoggerAdapter to pass to the connector
    connector_logger_adapter = logging.LoggerAdapter(target_logger, {'org_group': target_identifier})
    connector_logger_adapter.logger.propagate = False 
    
    # --- Determine the path for the previous scan's intermediate file for this specific target ---
    # This will be used as the cache input for the current scan of this target.
    previous_intermediate_filename = f"intermediate_{platform}_{target_identifier.replace('/', '_').replace('.', '_')}.json"
    previous_intermediate_filepath = os.path.join(cfg.OUTPUT_DIR, previous_intermediate_filename)
    connector_logger_adapter.info(f"--- Starting scan for {platform} target: {target_identifier} ---")
    if hours_per_commit is None or hours_per_commit == 0: hours_per_commit = None

    fetched_repos = []
    connector_success = False

    if limit_to_pass is not None and global_repo_counter[0] >= limit_to_pass:
        main_stream_logger.warning(f"Skipping {platform} target {target_identifier} due to global repository limit ({limit_to_pass}).")
        connector_logger_adapter.warning(f"Global debug limit ({limit_to_pass}) reached. Skipping scan for {target_identifier}.")
        connector_logger_adapter.info(f"--- Finished scan for {platform} target: {target_identifier} (Skipped due to limit) ---")
        return True

    try:
        if platform == "github":
            fetched_repos = clients.github_connector.fetch_repositories(
                token=auth_params.get("token"),
                org_name=target_identifier,
                processed_counter=global_repo_counter,
                processed_counter_lock=processed_counter_lock,
                logger_instance=connector_logger_adapter,
                debug_limit=limit_to_pass,
                github_instance_url=platform_url,
                hours_per_commit=hours_per_commit,
                max_workers=cfg.SCANNER_MAX_WORKERS_ENV,
                cfg_obj=cfg,
                previous_scan_output_file=previous_intermediate_filepath,
                pre_fetched_enriched_repos=pre_fetched_enriched_repos,
                global_inter_submission_delay=global_inter_submission_delay,
                gql_client_for_workers=gql_client_for_workers,
                graphql_endpoint_url_for_workers=graphql_endpoint_url_for_workers
            )
            connector_success = True
        elif platform == "gitlab":
            fetched_repos = clients.gitlab_connector.fetch_repositories(
                token=auth_params.get("token"),
                group_path=target_identifier,
                processed_counter=global_repo_counter,
                processed_counter_lock=processed_counter_lock,
                logger_instance=connector_logger_adapter.logger,
                debug_limit=limit_to_pass,
                gitlab_instance_url=platform_url,
                hours_per_commit=hours_per_commit,
                max_workers=cfg.SCANNER_MAX_WORKERS_ENV,
                cfg_obj=cfg,
                previous_scan_output_file=previous_intermediate_filepath,
                pre_fetched_enriched_repos=pre_fetched_enriched_repos,
                global_inter_submission_delay=global_inter_submission_delay
            )
            connector_success = True
        elif platform == "azure":
            if '/' not in target_identifier:
                 connector_logger_adapter.error(f"Invalid Azure DevOps target format: '{target_identifier}'. Expected Org/Project.")
                 main_stream_logger.error(f"Invalid Azure DevOps target format for {target_identifier}. Expected Org/Project. Skipping this target.")
                 return False
            fetched_repos = clients.azure_devops_connector.fetch_repositories(
                token=auth_params.get("pat_token"),
                target_path=target_identifier,
                processed_counter=global_repo_counter,
                processed_counter_lock=processed_counter_lock,
                logger_instance=connector_logger_adapter.logger,
                debug_limit=limit_to_pass,
                azure_devops_url=platform_url,
                hours_per_commit=hours_per_commit,
                max_workers=cfg.SCANNER_MAX_WORKERS_ENV,
                cfg_obj=cfg,
                previous_scan_output_file=previous_intermediate_filepath,
                spn_client_id=auth_params.get("spn_client_id"),
                spn_client_secret=auth_params.get("spn_client_secret"),
                spn_tenant_id=auth_params.get("spn_tenant_id"),
                pre_fetched_enriched_repos=pre_fetched_enriched_repos,
                global_inter_submission_delay=global_inter_submission_delay
            )
            connector_success = True
        else:
            connector_logger_adapter.error(f"Unknown platform: {platform}")
            main_stream_logger.error(f"Unknown platform: {platform} for target: {target_identifier}. Skipping this target.")
            return False

    except Exception as e:
        connector_logger_adapter.critical(f"Critical error during {platform} connector execution for {target_identifier}: {e}", exc_info=True)
        main_stream_logger.error(f"Critical error during connector execution for {platform} target {target_identifier}. See target-specific log for details. Skipping this target.")
        connector_logger_adapter.info(f"--- Finished scan for {platform} target: {target_identifier} (Critical Connector Error) ---")
        return False

    if connector_success:
        if fetched_repos:
            connector_logger_adapter.info(f"Connector returned {len(fetched_repos)} repositories for {target_identifier}.")
            main_stream_logger.info(f"Found {len(fetched_repos)} repositories for {platform} target: {target_identifier}.")
        else:
            connector_logger_adapter.info(f"Connector returned no repositories for {target_identifier}.")
            main_stream_logger.info(f"No repositories found to process for {platform} target: {target_identifier}.")

    default_org_ids_for_exemption_processor = [target_identifier]
    if platform == "azure" and '/' in target_identifier:
        default_org_ids_for_exemption_processor = [target_identifier.split('/',1)[0]]


    if not fetched_repos and connector_success :
        connector_logger_adapter.info(f"{ANSI_YELLOW}No repositories to process for {target_identifier} or limit reached within connector.{ANSI_RESET}")
        intermediate_data = []
    else:
        connector_logger_adapter.info(f"Finalizing {len(fetched_repos)} repositories for {target_identifier}...")
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
            connector_logger_adapter.info(f"Repo: mynodejs - Permissions content for intermediate file: {item_to_log.get('permissions')}", extra={'org_group': repo_org_group})
            break

    if write_json_file(intermediate_data, intermediate_filepath):
        connector_logger_adapter.info(f"Successfully wrote intermediate data to {intermediate_filepath}")
        connector_logger_adapter.info(f"--- Finished scan for {platform} target: {target_identifier} ---")
        return True
    else:
        connector_logger_adapter.error(f"Failed to write intermediate data for {target_identifier}.")
        connector_logger_adapter.info(f"--- Finished scan for {platform} target: {target_identifier} (Write Error) ---")
        return False

def _prepare_project_for_final_catalog(
    project_data: Dict[str, Any],
    platform: str,
    org: str,
    cfg: Config,
    main_logger: logging.Logger
) -> Optional[Dict[str, Any]]:
    """Prepares a single project's data for the final catalog, applying cleanup and checks."""
    now_iso = datetime.now(timezone.utc).isoformat()
    repo_platform =f"{platform}/{org}"

    if "processing_error" in project_data and len(project_data.keys()) <= 3: # e.g. name, org, error
        main_logger.warning(f"Keeping project entry with processing_error: {project_data.get('name', 'Unknown')}", extra={'org_group': repo_platform})
        return project_data # Keep error record

    updated_project_data = project_data.copy()

    is_public_repo = updated_project_data.get("repositoryVisibility", "").lower() == "public"
    is_empty_repo_flag = updated_project_data.get("_is_empty_repo", False)
    if is_public_repo and is_empty_repo_flag:
        main_logger.info(f"Skipping empty public repository during merge: {updated_project_data.get('name', 'UnknownRepo')}", extra={'org_group': repo_platform})
        return None # Skip adding this project

    if "date" not in updated_project_data or not isinstance(updated_project_data.get("date"), dict):
        updated_project_data["date"] = {}
    updated_project_data["date"]["metadataLastUpdated"] = now_iso

    repo_visibility_original = updated_project_data.get("repositoryVisibility", "").lower()
    if repo_visibility_original == "internal":
        updated_project_data["repositoryVisibility"] = "private"
        main_logger.debug(f"Repo {updated_project_data.get('name')}: Standardized visibility from 'internal' to 'private' for final output.", extra={'org_group': repo_platform})

    main_logger.debug(f"Repo {updated_project_data.get('name')}: Using privateID '{updated_project_data.get('privateID')}' from intermediate file.", extra={'org_group': repo_platform})

    # Cleanup internal/temporary fields
    for key_to_pop in ['_private_contact_emails', '_is_empty_repo', 'lastCommitSHA', 'repo_id']:
        updated_project_data.pop(key_to_pop, None)
    return updated_project_data

def merge_intermediate_catalogs(cfg: Config, main_logger: logging.Logger) -> bool:
    """
    Merges intermediate catalog files into a single catalog.
    Args:
        cfg (Config): Configuration object.
        main_logger (logging.Logger): Logger for main operations.
        returns:
        bool: True if merge operation is successful, False otherwise.
    """
    main_logger.setLevel(logging.info)
    main_logger.info("--- Starting Merge Operation ---")
    search_path = os.path.join(cfg.OUTPUT_DIR, INTERMEDIATE_FILE_PATTERN)
    intermediate_files = glob.glob(search_path)

    if not intermediate_files:
        main_logger.info(f"No intermediate catalog files found matching '{search_path}'. Nothing to merge.")
        return True

    main_logger.info(f"Found {len(intermediate_files)} intermediate catalog files to merge.")
    all_projects_raw = []
    merge_errors = False
    platform = ""
    org = ""

    # Get the data from each intermediate file first
    for filepath in intermediate_files:
        try:
            # Extract platform and org from filename
            filename = os.path.basename(filepath)
            name_part = filename.replace('intermediate_', '').replace('.json', '')
            platform, org = name_part.split('_', 1)
            
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, list):
                    # Add platform and org info to each project before extending the list
                    for project in data:
                        if isinstance(project, dict):
                            project['_source_platform'] = platform
                            project['_source_org'] = org
                    all_projects_raw.extend(data)
                else:
                    main_logger.warning(f"Content of {filepath} is not a list. Skipping.")
                    merge_errors = True
        except json.JSONDecodeError:
            main_logger.error(f"Error decoding JSON from {filepath}. Skipping.", exc_info=True)
            merge_errors = True
        except Exception as e:
            main_logger.error(f"Unexpected error processing {filepath}: {e}. Skipping.", exc_info=True)
            merge_errors = True

    if not all_projects_raw and intermediate_files:
        main_logger.warning("No data collected from intermediate files, though files were present.")
        return not merge_errors

    final_code_json_structure = {
        "version": CODE_JSON_SCHEMA_VERSION,
        "agency": cfg.AGENCY_NAME,
        "measurementType":  CODE_JSON_MEASUREMENT_TYPE,
        "projects": [] # Initialize projects as an empty list
    }

    # this loop will process each repo in all intermediate files found and add it to the final_code_json_structure
    processed_projects_for_final_catalog = []
    for project_data_raw in all_projects_raw:
        platform = project_data_raw.get('_source_platform', 'unknown')
        org = project_data_raw.get('_source_org', 'unknown')
        
        prepared_project = _prepare_project_for_final_catalog(project_data_raw,platform, org, cfg, main_logger)
        if prepared_project:
            processed_projects_for_final_catalog.append(prepared_project)

    # Backup existing catalog file (code.json)
    backup_existing_file(cfg.OUTPUT_DIR, cfg.CATALOG_JSON_FILE)

    final_code_json_structure["projects"] = sorted(processed_projects_for_final_catalog, key=lambda x: x.get("name", "").lower())

    final_catalog_filepath = os.path.join(cfg.OUTPUT_DIR, cfg.CATALOG_JSON_FILE)
    if write_json_file(final_code_json_structure, final_catalog_filepath):
        main_logger.info(f"Successfully merged {len(processed_projects_for_final_catalog)} projects into {final_catalog_filepath}")
        main_logger.info("--- Merge Operation Finished Successfully ---")

        stats = {
            "total_projects_in_catalog": 0,
            "public_projects_count": 0,
            "private_projects_count": 0,
            "exempted_projects_count": 0,
            "exemptions_by_type_overall": {},
            "exempted_count_by_organization": {}
        }

        for project_data in final_code_json_structure["projects"]:
            if "processing_error" in project_data and len(project_data.keys()) <= 3:
                continue

            stats["total_projects_in_catalog"] += 1
            org_name = project_data.get("organization", "UnknownOrganization")
            visibility = project_data.get("repositoryVisibility", "").lower()
            usage_type = project_data.get("permissions", {}).get("usageType", "")
            
            if visibility == "public":
                stats["public_projects_count"] += 1
            elif visibility == "private":
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
        return not merge_errors
    else:
        main_logger.error(f"Failed to write final merged catalog to {final_catalog_filepath}")
        main_logger.info("--- Merge Operation Finished With Errors ---")
        return False

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

def _prepare_azure_auth_params(args: argparse.Namespace, cfg: Config, main_logger: logging.Logger) -> Dict[str, Any]:
    """
    Prepares Azure DevOps authentication parameters based on CLI arguments and environment variables.
    Precedence: CLI SPN > CLI PAT > ENV SPN > ENV PAT.
    """
    params: Dict[str, Any] = {}

    spn_cid_cli = getattr(args, 'az_cid', None)
    spn_cs_cli = getattr(args, 'az_cs', None)
    spn_tid_cli = getattr(args, 'az_tid', None)
    if spn_cid_cli and spn_cs_cli and spn_tid_cli:
        if clients.azure_devops_connector.are_spn_details_placeholders(spn_cid_cli, spn_cs_cli, spn_tid_cli):
            main_logger.error("Azure Service Principal CLI arguments contain placeholders. Cannot proceed.")
            sys.exit(1)
        main_logger.info("Using Azure Service Principal credentials from CLI arguments.")
        params = {"spn_client_id": spn_cid_cli, "spn_client_secret": spn_cs_cli, "spn_tenant_id": spn_tid_cli}
        return params

    pat_cli = getattr(args, 'az_tk', None)
    if pat_cli:
        if clients.azure_devops_connector.is_placeholder_token(pat_cli):
            main_logger.error("Azure PAT from CLI argument is a placeholder. Cannot proceed.")
            sys.exit(1)
        main_logger.info("Using Azure PAT from CLI argument.")
        params["pat_token"] = pat_cli
        return params

    # Fallback to environment variables for PAT (SPN from env handled by connector if PAT fails)
    # If PAT is not valid from env, connector's _setup_azure_devops_credentials will try SPN from env.
    env_pat = os.getenv("AZURE_DEVOPS_TOKEN")
    if env_pat and not clients.azure_devops_connector.is_placeholder_token(env_pat):
        main_logger.info("Using Azure PAT from AZURE_DEVOPS_TOKEN environment variable.")
        params["pat_token"] = env_pat
    else: # No valid PAT from CLI or ENV, SPN from ENV will be tried by connector
        main_logger.info("Azure PAT not found or is placeholder in CLI/ENV. Will attempt SPN from ENV if configured.")
        # Ensure pat_token is None if not valid, so connector tries SPN from env
        params["pat_token"] = None

    # SPN details from ENV will be picked up by the connector if pat_token is None or invalid.
    # We don't need to explicitly pass them here if relying on connector's fallback.
    # However, to be explicit for the connector's _setup_azure_devops_credentials:
    params["spn_client_id"] = os.getenv("AZURE_CLIENT_ID")
    params["spn_client_secret"] = os.getenv("AZURE_CLIENT_SECRET")
    params["spn_tenant_id"] = os.getenv("AZURE_TENANT_ID")

    if not params.get("pat_token") and \
       clients.azure_devops_connector.are_spn_details_placeholders(params["spn_client_id"], params["spn_client_secret"], params["spn_tenant_id"]):
        main_logger.error("Azure DevOps authentication failed: Neither valid SPN details nor a PAT were provided from CLI or ENV, or they are placeholders.")
        sys.exit(1)

    return params

# --- Main CLI Function ---
def main_cli():
    script_start_time = time.time()

    cfg = Config()
    setup_global_logging() # Ensures logging is set up based on Config
    main_logger = logging.getLogger(__name__)
    main_logger_diag = main_logger # Use the same logger for diagnostics

    parser = argparse.ArgumentParser(description="Share IT Act Repository Scanning Tool")
    subparsers = parser.add_subparsers(dest="command", help="Available commands.", required=True)

    gh_parser = subparsers.add_parser("github", help="Scan GitHub organizations.")
    gh_parser.add_argument("--orgs", help="Comma-separated organizations to scan.")
    gh_parser.add_argument("--github-ghes-url", help="URL of the GitHub Enterprise Server instance.")
    gh_parser.add_argument("--gh-tk", help="GitHub Personal Access Token (PAT).")
    gh_parser.add_argument("--limit", type=int, help="Limit total repositories processed.")
    gh_parser.add_argument("--hours-per-commit", type=float, help="Hours to estimate per commit.")

    gl_parser = subparsers.add_parser("gitlab", help="Scan configured GitLab groups.")
    gl_parser.add_argument("--groups", help="Comma-separated GitLab groups/paths to scan.")
    gl_parser.add_argument("--gitlab-url", help="GitLab instance URL.")
    gl_parser.add_argument("--gl-tk", help="GitLab Personal Access Token (PAT).")
    gl_parser.add_argument("--limit", type=int, help="Limit total repositories processed.")
    gl_parser.add_argument("--hours-per-commit", type=float, help="Hours to estimate per commit.")

    az_parser = subparsers.add_parser("azure", help="Scan Azure DevOps Org/Project targets.")
    az_parser.add_argument("--targets", help="Comma-separated Azure Org/Project pairs.")
    az_parser.add_argument("--az-tk", help="Azure DevOps Personal Access Token (PAT).")
    az_parser.add_argument("--az-cid", help="Azure Service Principal Client ID.")
    az_parser.add_argument("--az-cs", help="Azure Service Principal Client Secret.")
    az_parser.add_argument("--az-tid", help="Azure Service Principal Tenant ID.")
    az_parser.add_argument("--limit", type=int, help="Limit total repositories processed.")
    az_parser.add_argument("--hours-per-commit", type=float, help="Hours to estimate per commit.")

    merge_parser = subparsers.add_parser("merge", help="Merge intermediate catalog files.")
    args = parser.parse_args()

    os.makedirs(cfg.OUTPUT_DIR, exist_ok=True)
    try:
        exemption_manager = ExemptionLogger(cfg.EXEMPTION_LOG_FILEPATH)
        repo_id_mapping_manager = RepoIdMappingManager(cfg.PRIVATE_ID_FILEPATH)
        main_logger.info("ExemptionLogger and RepoIdMappingManager initialized.")
    except Exception as mgr_err:
        main_logger.critical(f"Failed to initialize Exemption/RepoIdMapping managers: {mgr_err}", exc_info=True)
        sys.exit(1)

    overall_command_success = True
    global_repo_scan_counter = [0]
    global_repo_scan_counter_lock = threading.Lock()

    limit_for_scans = None
    cli_limit_val = getattr(args, 'limit', None)
    if cli_limit_val is not None:
        limit_for_scans = cli_limit_val if cli_limit_val > 0 else None
        main_logger.info(f"CLI override: Repository processing limit set to {limit_for_scans if limit_for_scans else 'unlimited'}.")
    elif cfg.DEBUG_REPO_LIMIT is not None:
        limit_for_scans = cfg.DEBUG_REPO_LIMIT if cfg.DEBUG_REPO_LIMIT > 0 else None
        main_logger.info(f"Using repository processing limit from .env: {limit_for_scans if limit_for_scans else 'unlimited'}.")
    else:
        main_logger.info("No repository processing limit set (processing all).")

    hours_per_commit_for_scan: Optional[float] = None
    cli_hpc_val = getattr(args, 'hours_per_commit', None)
    if cli_hpc_val is not None:
        hours_per_commit_for_scan = float(cli_hpc_val)
    elif cfg.HOURS_PER_COMMIT_ENV is not None:
        hours_per_commit_for_scan = cfg.HOURS_PER_COMMIT_ENV
    if hours_per_commit_for_scan is not None and hours_per_commit_for_scan > 0:
        main_logger.info(f"Labor hours estimation ENABLED. Hours per commit: {hours_per_commit_for_scan}.")
    else:
        main_logger.info(f"{ANSI_RED}Labor hours estimation DISABLED.{ANSI_RESET}")

    # --- Platform Specific Pre-Scan and Main Scan ---
    # Note: The _orchestrate_platform_scan function now handles the detailed logic
    # for pre-scan, rate limiting, and main scan for each platform.

    if args.command == "github":
        overall_command_success = _orchestrate_platform_scan(
            platform_name="github", args=args, cfg=cfg,
            repo_id_mapping_mgr=repo_id_mapping_manager, exemption_mgr=exemption_manager,
            global_repo_scan_counter=global_repo_scan_counter, global_repo_scan_counter_lock=global_repo_scan_counter_lock,
            limit_for_scans=limit_for_scans, hours_per_commit_for_scan=hours_per_commit_for_scan, main_logger=main_logger,
            auth_setup_func=_get_and_validate_token,
            target_retrieval_func=get_targets_from_cli_or_env,
            estimate_calls_func=clients.github_connector.estimate_api_calls_for_org,
            get_rate_limit_status_func=clients.github_connector.get_github_rate_limit_status,
            calculate_inter_submission_delay_func=clients.github_connector.calculate_inter_submission_delay,
            connector_module=clients.github_connector,
            cli_token_arg_name="gh_tk", env_token_var_name="GITHUB_TOKEN",
            cli_target_arg_name="orgs", env_target_cfg_attr_name="GITHUB_ORGS_ENV",
            platform_url_cli_arg_name="github_ghes_url",
            placeholder_check_func=clients.github_connector.is_placeholder_token,
            entity_name_plural="GitHub organizations",
            total_estimated_calls_cfg_attr="GITHUB_TOTAL_ESTIMATED_API_CALLS",
            requires_common_gql_client=True
        )
    elif args.command == "gitlab":
        overall_command_success = _orchestrate_platform_scan(
            platform_name="gitlab", args=args, cfg=cfg,
            repo_id_mapping_mgr=repo_id_mapping_manager, exemption_mgr=exemption_manager,
            global_repo_scan_counter=global_repo_scan_counter, global_repo_scan_counter_lock=global_repo_scan_counter_lock,
            limit_for_scans=limit_for_scans, hours_per_commit_for_scan=hours_per_commit_for_scan, main_logger=main_logger,
            auth_setup_func=_get_and_validate_token,
            target_retrieval_func=get_targets_from_cli_or_env,
            estimate_calls_func=clients.gitlab_connector.estimate_api_calls_for_group,
            get_rate_limit_status_func=clients.gitlab_connector.get_gitlab_rate_limit_status,
            calculate_inter_submission_delay_func=clients.gitlab_connector.calculate_inter_submission_delay,
            connector_module=clients.gitlab_connector,
            cli_token_arg_name="gl_tk", env_token_var_name="GITLAB_TOKEN",
            cli_target_arg_name="groups", env_target_cfg_attr_name="GITLAB_GROUPS_ENV",
            platform_url_cli_arg_name="gitlab_url", platform_url_cfg_attr_name="GITLAB_URL_ENV",
            placeholder_check_func=clients.gitlab_connector.is_placeholder_token,
            entity_name_plural="GitLab groups",
            total_estimated_calls_cfg_attr="GITLAB_TOTAL_ESTIMATED_API_CALLS"
        )
    elif args.command == "azure":
        overall_command_success = _orchestrate_platform_scan(
            platform_name="azure", args=args, cfg=cfg,
            repo_id_mapping_mgr=repo_id_mapping_manager, exemption_mgr=exemption_manager,
            global_repo_scan_counter=global_repo_scan_counter, global_repo_scan_counter_lock=global_repo_scan_counter_lock,
            limit_for_scans=limit_for_scans, hours_per_commit_for_scan=hours_per_commit_for_scan, main_logger=main_logger,
            auth_setup_func=_prepare_azure_auth_params,
            target_retrieval_func=get_targets_from_cli_or_env,
            target_parsing_func=parse_azure_targets_from_string_list,
            estimate_calls_func=clients.azure_devops_connector.estimate_api_calls_for_target,
            get_rate_limit_status_func=clients.azure_devops_connector.get_azure_devops_rate_limit_status,
            calculate_inter_submission_delay_func=clients.azure_devops_connector.calculate_inter_submission_delay,
            connector_module=clients.azure_devops_connector,
            cli_target_arg_name="targets", env_target_cfg_attr_name="AZURE_DEVOPS_TARGETS_RAW_ENV",
            platform_url_cfg_attr_name="AZURE_DEVOPS_API_URL_ENV", # Azure URL from cfg/env only
            entity_name_plural="Azure DevOps targets",
            total_estimated_calls_cfg_attr="AZURE_TOTAL_ESTIMATED_API_CALLS"
        )
    elif args.command == "merge":
        # Initialize privateid manager BEFORE backing up the file
        repo_id_mapping_manager = RepoIdMappingManager(cfg.PRIVATE_ID_FILEPATH)
        # THEN backup the file
        backup_existing_file(cfg.OUTPUT_DIR, cfg.PRIVATE_ID_FILENAME)
        if not merge_intermediate_catalogs(cfg, main_logger):
            overall_command_success = False
        main_logger.info("--- Merge Command Finished ---")

    try:
        main_logger.info("Saving Exemption logs and Repo ID mappings...")
        exemption_manager.save_all_exemptions()
        repo_id_mapping_manager.save_all_mappings()
        main_logger.info(f"Exemptions logged to: {cfg.EXEMPTION_LOG_FILEPATH}")
        main_logger.info(f"Private ID mappings saved to: {cfg.PRIVATE_ID_FILEPATH}")
    except Exception as save_err:
        main_logger.error(f"Error saving manager data: {save_err}", exc_info=True)
        overall_command_success = False

    script_end_time = time.time()
    total_duration_seconds = script_end_time - script_start_time
    formatted_duration = format_duration(total_duration_seconds)
    main_logger.info(f"Total script execution time: {formatted_duration}.")

    if overall_command_success:
        main_logger.info(f"Command '{args.command}' completed successfully.")
    else:
        main_logger.error(f"Command '{args.command}' encountered errors. Please check logs.")
    main_logger.info("----------------------------------------------------------------------")
    sys.exit(0 if overall_command_success else 1)

if __name__ == "__main__":
    main_cli()
