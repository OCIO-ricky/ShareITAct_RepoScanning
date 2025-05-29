# clients/gitlab_connector.py
"""
GitLab Connector for Share IT Act Repository Scanning Tool.

This module is responsible for fetching repository data from a GitLab instance,
including metadata, README content, CODEOWNERS files (if found), topics (tags),
and Git tags. It interacts with the GitLab API via the python-gitlab library.
"""

import os
import logging
import time
import threading # For locks
from concurrent.futures import ThreadPoolExecutor, as_completed
import base64
from typing import List, Optional, Dict, Any
from datetime import timezone, datetime 
from utils.delay_calculator import calculate_dynamic_delay # Import the calculator
from utils.caching import load_previous_scan_data, PLATFORM_CACHE_CONFIG
from utils.dateparse import parse_repos_created_after_date # Import the new utility
from utils.labor_hrs_estimator import analyze_gitlab_repo_sync # Import the labor hrs estimator
from utils.fetch_utils import (
    # These might be less needed if GQL handles retries or has different error patterns
    fetch_optional_content_with_retry,
    FETCH_ERROR_FORBIDDEN, FETCH_ERROR_NOT_FOUND, FETCH_ERROR_EMPTY_REPO_API,
    FETCH_ERROR_API_ERROR, FETCH_ERROR_UNEXPECTED
)


import gitlab # python-gitlab library
from .graphql_clients import gitlab_gql # Import the new GitLab GraphQL client
from gitlab.exceptions import GitlabAuthenticationError, GitlabGetError, GitlabListError, GitlabError, GitlabHttpError

# ANSI escape codes for coloring output
ANSI_RED = "\x1b[31;1m"  # Bold Red
ANSI_YELLOW = "\x1b[33;1m" # Corrected typo from 333
ANSI_RESET = "\x1b[0m"   # Reset to default color

# Attempt to import the exemption processor
try:
    from utils import exemption_processor
except ImportError:
    # Provide a mock if not found, so the connector can still be outlined
    logging.getLogger(__name__).error(
        "Failed to import exemption_processor from utils. "
        "Exemption processing will be skipped by the GitLab connector (using mock)."
    )
    class MockExemptionProcessor:
        def process_repository_exemptions(self, repo_data: Dict[str, Any], default_org_identifiers: Optional[List[str]] = None, **kwargs: Any) -> Dict[str, Any]:
            repo_data.setdefault('_status_from_readme', None)
            repo_data.setdefault('_private_contact_emails', [])
            repo_data.setdefault('contact', {})
            repo_data.setdefault('permissions', {"usageType": "openSource"})
            repo_data.pop('readme_content', None)
            repo_data.pop('_codeowners_content', None)
            # repo_data.pop('is_empty_repo', None) # _is_empty_repo is a valid field
            return repo_data
    exemption_processor = MockExemptionProcessor()

logger = logging.getLogger(__name__)

PLACEHOLDER_GITLAB_TOKEN = "YOUR_GITLAB_PAT"

# --- Constants for the fetch_utils utility ---
GITLAB_EXCEPTION_MAP = {
    'forbidden_exception': lambda e: isinstance(e, GitlabGetError) and e.response_code == 403,
    'not_found_exception': lambda e: isinstance(e, GitlabGetError) and e.response_code == 404,
    'empty_repo_check_func': lambda e: (
        isinstance(e, GitlabGetError) and e.response_code == 404 and
        ("empty repository" in str(e).lower() or "File Not Found" in str(e))
    ),
    'generic_platform_exception': gitlab.exceptions.GitlabError
}
MAX_QUICK_CONTENT_RETRIES_GITLAB = 2
QUICK_CONTENT_RETRY_DELAY_SECONDS_GITLAB = 3

def is_placeholder_token(token: Optional[str]) -> bool:
    """Checks if the GitLab token is missing or a known placeholder."""
    return not token or token == PLACEHOLDER_GITLAB_TOKEN

def _process_single_gitlab_project(
    project_stub_id: int,
    group_full_path: str, # The full path of the parent group
    token: Optional[str],
    effective_gitlab_url: str, # URL for creating the Gitlab instance
    ssl_verify_for_client: bool, # SSL verification flag for the client
    hours_per_commit: Optional[float],
    cfg_obj: Any,
    inter_repo_adaptive_delay_seconds: float,
    dynamic_post_api_call_delay_seconds: float,
    previous_scan_cache: Dict[str, Dict], # project_id_str (numeric) is key
    current_commit_sha: Optional[str], # This is from REST stub's last_activity_at commit
    num_repos_in_target: Optional[int], # Added for dynamic delay calculation in REST calls
    num_workers: int = 1,
    logger_instance: Optional[logging.LoggerAdapter] = None # Accept a logger instance
) -> Dict[str, Any]:
    """
    Processes a single GitLab project to extract its metadata.
    This function is intended to be run in a separate thread.
    """
    repo_data: Dict[str, Any] = {
        "repo_id": project_stub_id, # Initialize with the input numeric project ID
        "organization": group_full_path, # Initialize with the input group path
        "name": f"ID_{project_stub_id}" # Default name, will be updated if possible
    }
    project_rest_stub: Optional[gitlab.objects.Project] = None # For initial ID and path
    gitlab_cache_config = PLATFORM_CACHE_CONFIG["gitlab"]
    
    # Use passed-in logger or get a default one for this module
    current_logger = logger_instance if logger_instance else logging.getLogger(__name__)

    try:
        # Create a new Gitlab instance for this specific thread/task
        gl_instance_for_task = gitlab.Gitlab(effective_gitlab_url, private_token=token, ssl_verify=ssl_verify_for_client, timeout=30)
        try:
            gl_instance_for_task.auth() # Verify authentication
        except Exception as auth_e:
            current_logger.error(f"GitLab client authentication failed for URL {effective_gitlab_url} in thread: {auth_e}")
            # repo_id, organization, and default name are already set in repo_data
            repo_data["processing_error"] = f"GitLab client auth failed: {auth_e}"
            return repo_data

        # Get minimal project object (stub) for ID and path_with_namespace
        # This REST call is still needed to get project_full_path for GQL and numeric ID for caching.
        # Set lazy=False to ensure attributes like path_with_namespace are loaded.
        project_rest_stub = gl_instance_for_task.projects.get(project_stub_id, lazy=False)
        repo_data["name"] = project_rest_stub.path # Update name from project path (name without namespace)
        if project_rest_stub.id != project_stub_id:
            current_logger.warning(f"Mismatch between input project_stub_id ({project_stub_id}) and fetched project_rest_stub.id ({project_rest_stub.id}) for {project_rest_stub.path_with_namespace}. Using input ID ({project_stub_id}) for repo_data['repo_id'].")
            # repo_data["repo_id"] is already set to project_stub_id, which is intended.

        # --- Fetch License Info via REST API ---
        # The license field is not reliably available via GraphQL on this instance.
        # Use the REST API endpoint with the 'license' flag.
        license_info_rest: Optional[Dict[str, Any]] = None
        try:
            # Apply dynamic delay before this REST call
            if dynamic_post_api_call_delay_seconds > 0:
                 current_logger.debug(f"GitLab applying SYNC REST call delay (get project license): {dynamic_post_api_call_delay_seconds:.2f}s")
                 time.sleep(dynamic_post_api_call_delay_seconds)

            # Fetch the project again, specifically requesting license info
            project_with_license = gl_instance_for_task.projects.get(project_stub_id, license=1, lazy=False)
            license_info_rest = project_with_license.license # This is a dict if license found, else None
            if license_info_rest: current_logger.debug(f"Successfully fetched license info via REST for {project_rest_stub.path_with_namespace}")
        except GitlabGetError as e_license:
            current_logger.warning(f"GitLab API error fetching license for project {project_rest_stub.path_with_namespace}: {e_license}. Proceeding without license info.", exc_info=False)
        except Exception as e_license_unexpected:
             current_logger.warning(f"Unexpected error fetching license for project {project_rest_stub.path_with_namespace}: {e_license_unexpected}. Proceeding without license info.", exc_info=True)
        # --- End Fetch License Info via REST API ---

        # --- Caching Logic ---
        # Use project_stub_id (which is repo_data["repo_id"]) for cache key consistency
        if current_commit_sha: # Only attempt cache hit if we have a current SHA to compare
            cached_repo_entry = previous_scan_cache.get(str(project_stub_id))
            if cached_repo_entry:
                cached_commit_sha = cached_repo_entry.get(gitlab_cache_config["commit_sha_field"])
                if cached_commit_sha and current_commit_sha == cached_commit_sha: # Compare with SHA from REST stub
                    current_logger.info(f"CACHE HIT (pre-GraphQL): GitLab project '{project_rest_stub.path_with_namespace}' (ID: {project_stub_id}) has not changed based on REST SHA. Using cached data.")
                    repo_data_to_process = cached_repo_entry.copy()
                    repo_data_to_process[gitlab_cache_config["commit_sha_field"]] = current_commit_sha
                    if "repo_id" not in repo_data_to_process and "id" in repo_data_to_process:
                        repo_data_to_process["repo_id"] = int(repo_data_to_process["id"]) # Ensure it's int
                    repo_data_to_process["repo_id"] = project_stub_id # Ensure the correct ID is used
                    repo_data_to_process["organization"] = group_full_path # Ensure org is correct
                    if cfg_obj:
                        repo_data_to_process = exemption_processor.process_repository_exemptions(
                            repo_data_to_process,
                            scm_org_for_logging=group_full_path, 
                            cfg_obj=cfg_obj,
                            default_org_identifiers=[group_full_path]
                    )
                    return repo_data_to_process

        current_logger.info(f"CACHE MISS or no SHA (pre-GraphQL): Processing GitLab project: {project_rest_stub.path_with_namespace} (ID: {project_stub_id}) with GraphQL full data fetch.")

        gql_data = gitlab_gql.fetch_project_details_graphql(
            gl_instance_for_task, # Use the task-specific client
            project_rest_stub.path_with_namespace,
            default_branch=project_rest_stub.default_branch,
            logger_instance=current_logger # Pass the logger
        )

        if not gql_data:
            current_logger.error(f"Failed to fetch GraphQL data for {project_rest_stub.path_with_namespace}. Skipping.")
            # repo_id, organization, and name are already set in repo_data
            repo_data["processing_error"] = "GraphQL data fetch failed"
            return repo_data

        repo_full_name = gql_data.get("fullPath", project_rest_stub.path_with_namespace)
        # Update name from GQL if available and different, otherwise keep the one from project_rest_stub.path
        repo_data["name"] = gql_data.get("name", repo_data["name"])

        # Check if it's a fork using the REST API stub
        if hasattr(project_rest_stub, 'forked_from_project') and project_rest_stub.forked_from_project:
            # project_rest_stub.forked_from_project is a dict with parent project details
            # e.g., {'id': ..., 'name': ..., 'path_with_namespace': 'group/parent_project'}
            parent_project_path = project_rest_stub.forked_from_project.get('path_with_namespace', 'unknown source')
            current_logger.info(f"Skipping forked repository: {repo_full_name} (fork of {parent_project_path})")
            # repo_id, organization, and name are already set
            repo_data["processing_status"] = "skipped_fork"
            return repo_data

        repo_data['_is_empty_repo'] = False
        gql_repo_node = gql_data.get("repository", {})
        if gql_repo_node.get("empty", False):
            current_logger.info(f"Repository {repo_full_name} is marked as empty by GitLab API.")
            repo_data['_is_empty_repo'] = True

        repo_description = gql_data.get("description") or ""
        visibility_status = gql_data.get("visibility", "private").lower()
        if visibility_status not in ["public", "private", "internal"]:
            current_logger.warning(f"Unknown visibility '{visibility_status}' for {repo_full_name}. Defaulting to 'private'.")
            visibility_status = "private"

        created_at_dt: Optional[datetime] = None
        created_at_str = gql_data.get("createdAt")
        if created_at_str:
            try:
                created_at_dt = datetime.fromisoformat(created_at_str.replace('Z', '+00:00')).replace(tzinfo=timezone.utc)
            except ValueError:
                current_logger.warning(f"Could not parse created_at date string '{created_at_str}' for {repo_full_name}")

        last_activity_at_dt: Optional[datetime] = None
        last_activity_at_str = gql_data.get("lastActivityAt")
        if last_activity_at_str:
            try:
                last_activity_at_dt = datetime.fromisoformat(last_activity_at_str.replace('Z', '+00:00')).replace(tzinfo=timezone.utc)
            except ValueError:
                current_logger.warning(f"Could not parse last_activity_at date string '{last_activity_at_str}' for {repo_full_name}")

        all_languages_list = [lang_entry["name"] for lang_entry in gql_data.get("languages", []) if lang_entry and lang_entry.get("name")]

        readme_content: Optional[str] = None
        readme_html_url: Optional[str] = None
        default_branch_for_url = gql_repo_node.get("rootRef")

        for i, path in enumerate(gitlab_gql.COMMON_README_PATHS_GITLAB):
            alias_prefix = "readme"
            alias_name = path.replace('.', '_').replace('/', '_')
            # blobs field returns a connection object which has a 'nodes' list.
            blob_connection_data = gql_repo_node.get(f"{alias_prefix}_{alias_name}_{i}")
            if blob_connection_data and isinstance(blob_connection_data, dict):
                blob_nodes = blob_connection_data.get("nodes")
                if blob_nodes and isinstance(blob_nodes, list) and len(blob_nodes) > 0:
                    actual_blob_data = blob_nodes[0] # Get the first blob from the nodes list
                    if actual_blob_data and actual_blob_data.get("rawTextBlob") is not None:
                        readme_content = actual_blob_data["rawTextBlob"]
                        if gql_data.get("webUrl") and actual_blob_data.get("webPath"):
                            readme_html_url = f"{gql_data['webUrl'].rstrip('/')}{actual_blob_data['webPath']}"
                        current_logger.debug(f"Found README '{path}' for {repo_full_name} via GitLab GraphQL.")
                        break
        if not readme_content: current_logger.debug(f"No common README file found for {repo_full_name} via GitLab GraphQL.")

        codeowners_content: Optional[str] = None
        for i, path in enumerate(gitlab_gql.COMMON_CODEOWNERS_PATHS_GITLAB):
            alias_prefix = "codeowners"
            alias_name = path.replace('.', '_').replace('/', '_')
            # blobs field returns a connection object which has a 'nodes' list.
            blob_connection_data = gql_repo_node.get(f"{alias_prefix}_{alias_name}_{i}")
            if blob_connection_data and isinstance(blob_connection_data, dict):
                blob_nodes = blob_connection_data.get("nodes")
                if blob_nodes and isinstance(blob_nodes, list) and len(blob_nodes) > 0:
                    actual_blob_data = blob_nodes[0] # Get the first blob from the nodes list
                    if actual_blob_data and actual_blob_data.get("rawTextBlob") is not None:
                        codeowners_content = actual_blob_data["rawTextBlob"]
                        current_logger.debug(f"Found CODEOWNERS '{path}' for {repo_full_name} via GitLab GraphQL.")
                        break
        if not codeowners_content: current_logger.debug(f"No CODEOWNERS file found for {repo_full_name} via GitLab GraphQL.")

        repo_topics = gql_data.get("topics", [])
        repo_git_tags = [node["tagName"] for node in gql_data.get("releases", {}).get("nodes", []) if node and node.get("tagName")]

        licenses_list = []
        # Use license info fetched via REST API
        # The REST API returns a 'license' dict with 'spdx_identifier', 'name', 'html_url', etc.
        if license_info_rest and license_info_rest.get("spdx_identifier"):
            # Map REST fields to code.gov schema fields
            # Corrected to use license_info_rest and its typical keys
            license_entry = {"spdxID": license_info_rest.get('key') or license_info_rest.get('spdx_identifier'), "name": license_info_rest.get('name')}
            licenses_list.append({k: v for k, v in license_entry.items() if v})

        repo_data.update({
            "description": repo_description,
            "repositoryURL": gql_data.get("webUrl"),
            "homepageURL": gql_data.get("webUrl"),
            "downloadURL": None, 
            "readme_url": readme_html_url, 
            "vcs": "git", 
            "repositoryVisibility": visibility_status,
            "status": "development", "version": "N/A", "laborHours": 0, 
            "languages": all_languages_list,
            "tags": repo_topics,
            "date": {
                "created": created_at_dt.isoformat() if created_at_dt else None,
                "lastModified": last_activity_at_dt.isoformat() if last_activity_at_dt else None,
            },
            "permissions": {"usageType": None, "exemptionText": None, "licenses": licenses_list},
            "contact": {}, "contractNumber": None, 
            "readme_content": readme_content,
            "_codeowners_content": codeowners_content,
            "_api_tags": repo_git_tags, 
            "archived": gql_data.get("archived", False)
        })
        repo_data.setdefault('_is_empty_repo', False)

        gql_latest_commit_sha = None
        if gql_repo_node.get("tree", {}).get("lastCommit"):
            gql_latest_commit_sha = gql_repo_node["tree"]["lastCommit"].get("sha")
        
        if gql_latest_commit_sha:
            repo_data[gitlab_cache_config["commit_sha_field"]] = gql_latest_commit_sha
        elif current_commit_sha:
             repo_data[gitlab_cache_config["commit_sha_field"]] = current_commit_sha

        if hours_per_commit is not None:
            current_logger.debug(f"Estimating labor hours for GitLab repo: {repo_full_name}")
            labor_df = analyze_gitlab_repo_sync(
                project_id=str(project_rest_stub.id), token=token,
                hours_per_commit=hours_per_commit, 
                gitlab_api_url=effective_gitlab_url,
                gl_instance_for_gql=gl_instance_for_task, # Pass the client for GQL commit history
                default_branch_override=project_rest_stub.default_branch, # Pass default branch
                cfg_obj=cfg_obj, # Pass cfg_obj for its own delay calculations
                num_repos_in_target=num_repos_in_target, # For dynamic delay in commit fetching
                is_empty_repo=repo_data.get('_is_empty_repo', False),
                number_of_workers=num_workers,
                logger_instance=current_logger # Pass logger
            )
            repo_data["laborHours"] = round(float(labor_df["EstimatedHours"].sum()), 2) if not labor_df.empty else 0.0
            if repo_data["laborHours"] > 0: current_logger.info(f"Estimated labor hours for {repo_full_name}: {repo_data['laborHours']}")
        
        if cfg_obj:
            repo_data = exemption_processor.process_repository_exemptions(
                repo_data,
                scm_org_for_logging=group_full_path, 
                cfg_obj=cfg_obj,
                default_org_identifiers=[group_full_path])
        else:
            current_logger.warning(f"cfg_obj not provided to _process_single_gitlab_project for {repo_full_name}.")
            repo_data = exemption_processor.process_repository_exemptions(
                repo_data,
                scm_org_for_logging=group_full_path, 
                cfg_obj=cfg_obj, # No cfg_obj, so no adaptive delay or other cfg-based processing
                default_org_identifiers=[group_full_path])
        if inter_repo_adaptive_delay_seconds > 0:
            current_logger.debug(f"GitLab project {repo_full_name}: Applying INTER-REPO adaptive delay of {inter_repo_adaptive_delay_seconds:.2f}s")
            time.sleep(inter_repo_adaptive_delay_seconds)

        return repo_data

    except GitlabGetError as p_get_err:
        # project_rest_stub might be None if gl_instance_for_task.projects.get() failed
        # repo_data["name"] would still be the default f"ID_{project_stub_id}"
        name_for_log = project_rest_stub.path_with_namespace if project_rest_stub and hasattr(project_rest_stub, 'path_with_namespace') else repo_data['name']
        current_logger.error(f"GitLab API error getting details for project {name_for_log} (ID: {project_stub_id}): {p_get_err}. Skipping.", exc_info=False)
        # repo_id, organization, and default name are already set in repo_data
        repo_data["processing_error"] = f"GitLab API GetError: {p_get_err.error_message if hasattr(p_get_err, 'error_message') else str(p_get_err)}"
        return repo_data
    except Exception as e_proj:
        name_for_log = project_rest_stub.path_with_namespace if project_rest_stub and hasattr(project_rest_stub, 'path_with_namespace') else repo_data['name']
        current_logger.error(f"Unexpected error processing project {name_for_log} (ID: {project_stub_id}): {e_proj}. Skipping.", exc_info=True)
        # repo_id, organization, and default name are already set in repo_data
        repo_data["processing_error"] = f"Unexpected Error: {str(e_proj)}"
        return repo_data


def _parse_gitlab_iso_datetime_for_filter(datetime_str: Optional[str], logger_instance: logging.Logger, repo_name_for_log: str, field_name: str) -> Optional[datetime]:
    """Parses GitLab's ISO datetime string to a timezone-aware datetime object for filtering."""
    if not datetime_str:
        return None
    try:
        dt_obj = datetime.fromisoformat(datetime_str.replace('Z', '+00:00'))
        return dt_obj.replace(tzinfo=timezone.utc) if dt_obj.tzinfo is None else dt_obj
    except ValueError:
        logger_instance.warning(f"Could not parse {field_name} date string '{datetime_str}' for {repo_name_for_log} during filter check.")
        return None

def fetch_repositories(
    token: Optional[str], 
    group_path: str, 
    processed_counter: List[int], 
    processed_counter_lock: threading.Lock,
    debug_limit: int | None = None, 
    gitlab_instance_url: str | None = None,
    hours_per_commit: Optional[float] = None,
    max_workers: int = 5, 
    cfg_obj: Optional[Any] = None,
    previous_scan_output_file: Optional[str] = None
) -> list[dict]:
    """
    Fetches repository (project) details from a specific GitLab group.
    """
    effective_gitlab_url_for_threads = gitlab_instance_url
    if not effective_gitlab_url_for_threads:
        # Use the module-level logger for setup messages before specific group context is known
        module_logger = logging.getLogger(__name__)

        effective_gitlab_url_for_threads = cfg_obj.GITLAB_URL_ENV if cfg_obj and hasattr(cfg_obj, 'GITLAB_URL_ENV') else "https://gitlab.com"
        if effective_gitlab_url_for_threads == "https://gitlab.com": # Only log warning if it defaulted to public
            logger.warning(f"No GitLab instance URL provided or in cfg_obj.GITLAB_URL_ENV. Using default: {effective_gitlab_url_for_threads}")
    
    logger.info(f"Attempting to fetch repositories CONCURRENTLY for GitLab group: {group_path} on {effective_gitlab_url_for_threads} (max_workers: {max_workers})")

    if is_placeholder_token(token):
        logging.getLogger(__name__).error("GitLab token is a placeholder or missing. Cannot fetch repositories.") # Use specific logger
        return []
    if not group_path:
        logging.getLogger(__name__).warning("GitLab group path not provided. Skipping GitLab scan.") # Use specific logger
        return []
    
    repos_created_after_filter_date: Optional[datetime] = None
    if cfg_obj and hasattr(cfg_obj, 'REPOS_CREATED_AFTER_DATE'):
        repos_created_after_filter_date = parse_repos_created_after_date(cfg_obj.REPOS_CREATED_AFTER_DATE, logger)

    ssl_verify_flag = True
    disable_ssl_env = os.getenv("DISABLE_SSL_VERIFICATION", "false").lower()
    if disable_ssl_env == "true":
        ssl_verify_flag = False
        logging.getLogger(__name__).warning(f"{ANSI_RED}SECURITY WARNING: SSL certificate verification is DISABLED for GitLab connections.{ANSI_RESET}")

    previous_scan_cache: Dict[str, Dict] = {}
    if previous_scan_output_file:
        logging.getLogger(__name__).info(f"Attempting to load previous GitLab scan data for group '{group_path}' from: {previous_scan_output_file}")
        loaded_cache = load_previous_scan_data(previous_scan_output_file, "gitlab")
        if isinstance(loaded_cache, dict): previous_scan_cache = loaded_cache
        else: logger.warning(f"CACHE: load_previous_scan_data did not return a dict for {previous_scan_output_file}. Cache will be empty.")

    processed_repo_list: List[Dict[str, Any]] = []
    gl_for_listing: Optional[gitlab.Gitlab] = None

    try:
        gl_for_listing = gitlab.Gitlab(effective_gitlab_url_for_threads.strip('/'), private_token=token, timeout=30, ssl_verify=ssl_verify_flag)
        gl_for_listing.auth() 
        # Use module logger for initial connection messages
        logging.getLogger(__name__).info(f"Successfully connected and authenticated to GitLab instance for listing: {effective_gitlab_url_for_threads}")

        group = gl_for_listing.groups.get(group_path, lazy=False)
        
        # Create a LoggerAdapter with the group context
        # Use the module's logger as the base for the adapter
        group_specific_logger = logging.LoggerAdapter(logging.getLogger(__name__), {'org_group': group.full_path})
        group_specific_logger.info(f"Successfully found GitLab group: ID: {group.id}")

        num_projects_in_target_for_delay_calc = 0
        inter_repo_adaptive_delay_per_repo = 0.0
        live_project_stubs_materialized = None

        cached_project_count_for_target = 0
        if previous_scan_cache:
            gitlab_id_field = PLATFORM_CACHE_CONFIG.get("gitlab", {}).get("id_field", "repo_id")
            valid_cached_projects = [
                proj_data for proj_id, proj_data in previous_scan_cache.items()
                if isinstance(proj_data, dict) and proj_data.get(gitlab_id_field) is not None
            ]
            cached_project_count_for_target = len(valid_cached_projects)
            if cached_project_count_for_target > 0:
                num_projects_in_target_for_delay_calc = cached_project_count_for_target
                if repos_created_after_filter_date and cfg_obj and hasattr(cfg_obj, 'ADAPTIVE_DELAY_CACHE_MODIFIED_FACTOR_ENV'):
                    try:
                        total_live_projects_for_adjustment = len(list(group.projects.list(all=True, include_subgroups=True, statistics=False, lazy=True)))
                        if total_live_projects_for_adjustment > cached_project_count_for_target:
                            diff_count = total_live_projects_for_adjustment - cached_project_count_for_target
                            additional_projects_estimate = int(diff_count * float(cfg_obj.ADAPTIVE_DELAY_CACHE_MODIFIED_FACTOR_ENV))
                            if additional_projects_estimate > 0: num_projects_in_target_for_delay_calc += additional_projects_estimate
                    except Exception as e_adj_count: group_specific_logger.warning(f"Error fetching total live project count for cache adjustment: {e_adj_count}.")
                group_specific_logger.info(f"ADAPTIVE DELAY/PROCESSING: Using {num_projects_in_target_for_delay_calc} (cached, possibly adjusted) for target.")


        if num_projects_in_target_for_delay_calc == 0:
            try:
                group_specific_logger.info(f"ADAPTIVE DELAY/PROCESSING: Fetching live project list to get count.")
                all_live_project_stubs = list(group.projects.list(all=True, include_subgroups=True, statistics=False, lazy=True))
                initial_live_count = len(all_live_project_stubs)
                group_specific_logger.info(f"ADAPTIVE DELAY/PROCESSING: Fetched {initial_live_count} live projects before date filtering.")
                if repos_created_after_filter_date and all_live_project_stubs:
                    filtered_live_projects = []
                    skipped_legacy_count = 0
                    for proj_stub_item in all_live_project_stubs:
                        visibility = proj_stub_item.visibility
                        if visibility == "public":
                            filtered_live_projects.append(proj_stub_item)
                            continue
                        created_at_dt = _parse_gitlab_iso_datetime_for_filter(proj_stub_item.created_at, group_specific_logger, proj_stub_item.path_with_namespace, "created_at")
                        modified_at_dt = _parse_gitlab_iso_datetime_for_filter(proj_stub_item.last_activity_at, group_specific_logger, proj_stub_item.path_with_namespace, "last_activity_at")
                        if (created_at_dt and created_at_dt >= repos_created_after_filter_date) or \
                           (modified_at_dt and modified_at_dt >= repos_created_after_filter_date):
                            filtered_live_projects.append(proj_stub_item)
                        else: skipped_legacy_count += 1
                    live_project_stubs_materialized = filtered_live_projects
                    if skipped_legacy_count > 0: group_specific_logger.info(f"Skipped {skipped_legacy_count} non-public legacy projects due to date filter.")
                else: live_project_stubs_materialized = all_live_project_stubs
                num_projects_in_target_for_delay_calc = len(live_project_stubs_materialized)
                group_specific_logger.info(f"ADAPTIVE DELAY/PROCESSING: Using API count of {num_projects_in_target_for_delay_calc} (after date filter) for target.")
            except Exception as e_live_count: # This logger is module level, but group_specific_logger should be available
                group_specific_logger.warning(f"Error fetching live project list for count: {e_live_count}. num_projects_in_target_for_delay_calc will be 0.")
                num_projects_in_target_for_delay_calc = 0

        if cfg_obj and cfg_obj.ADAPTIVE_DELAY_ENABLED_ENV and num_projects_in_target_for_delay_calc > 0:
            if num_projects_in_target_for_delay_calc > cfg_obj.ADAPTIVE_DELAY_THRESHOLD_REPOS_ENV:
                excess_repos = num_projects_in_target_for_delay_calc - cfg_obj.ADAPTIVE_DELAY_THRESHOLD_REPOS_ENV
                scale_factor = 1 + (excess_repos / cfg_obj.ADAPTIVE_DELAY_THRESHOLD_REPOS_ENV)
                calculated_delay = cfg_obj.ADAPTIVE_DELAY_BASE_SECONDS_ENV * scale_factor
                inter_repo_adaptive_delay_per_repo = min(calculated_delay, cfg_obj.ADAPTIVE_DELAY_MAX_SECONDS_ENV)
                if inter_repo_adaptive_delay_per_repo > 0: group_specific_logger.info(f"{ANSI_YELLOW}INTER-REPO adaptive delay: {inter_repo_adaptive_delay_per_repo:.2f}s per project.{ANSI_RESET}")
        elif cfg_obj and num_projects_in_target_for_delay_calc > 0: group_specific_logger.info(f"Adaptive delay not applied.")
        
        dynamic_post_api_call_delay_seconds = 0.0
        if cfg_obj:
            base_delay_gql = float(getattr(cfg_obj, 'GITLAB_GRAPHQL_CALL_DELAY_SECONDS_ENV', os.getenv("GITLAB_GRAPHQL_CALL_DELAY_SECONDS", "0.2")))
            max_d_gql = float(getattr(cfg_obj, 'GITLAB_GRAPHQL_MAX_DELAY_SECONDS_ENV', os.getenv("GITLAB_GRAPHQL_MAX_DELAY_SECONDS", "0.5")))
            base_delay_rest = float(getattr(cfg_obj, 'GITLAB_POST_API_CALL_DELAY_SECONDS_ENV', os.getenv("GITLAB_POST_API_CALL_DELAY_SECONDS", "0.1")))
            
            # For GitLab, the main data fetch is GQL, but initial SHA fetch might be REST.
            # We'll use GQL delay settings for the dynamic_post_api_call_delay_seconds passed to _process_single_gitlab_project
            # as that's where the main GQL call happens.
            threshold = int(getattr(cfg_obj, 'DYNAMIC_DELAY_THRESHOLD_REPOS_ENV', os.getenv("DYNAMIC_DELAY_THRESHOLD_REPOS", "100")))
            scale = float(getattr(cfg_obj, 'DYNAMIC_DELAY_SCALE_FACTOR_ENV', os.getenv("DYNAMIC_DELAY_SCALE_FACTOR", "1.5")))
            
            dynamic_post_api_call_delay_seconds = calculate_dynamic_delay(
                base_delay_seconds=base_delay_gql, # Use GQL base delay for the main processing step
                num_items=num_projects_in_target_for_delay_calc if num_projects_in_target_for_delay_calc > 0 else None,
                threshold_items=threshold, scale_factor=scale, max_delay_seconds=max_d_gql,
                num_workers=max_workers
            )
            if dynamic_post_api_call_delay_seconds > 0:
                 group_specific_logger.info(f"{ANSI_YELLOW}DYNAMIC GQL_CALL delay set to: {dynamic_post_api_call_delay_seconds:.2f}s.{ANSI_RESET}")
            # Apply a REST delay for the initial listing if needed
            if live_project_stubs_materialized is None: # i.e., we are about to list projects
                rest_listing_delay = calculate_dynamic_delay(
                    base_delay_seconds=base_delay_rest,
                    num_items=num_projects_in_target_for_delay_calc if num_projects_in_target_for_delay_calc > 0 else None,
                    threshold_items=threshold, scale_factor=scale, max_delay_seconds=cfg_obj.ADAPTIVE_DELAY_MAX_SECONDS_ENV, # Use general max for REST
                    num_workers=max_workers
                )
                if rest_listing_delay > 0:
                    group_specific_logger.debug(f"Applying SYNC REST listing delay: {rest_listing_delay:.2f}s")
                    time.sleep(rest_listing_delay)


        project_count_for_group_submitted = 0
        skipped_by_date_filter_count = 0

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_project_name = {}
            try:
                projects_iterator = live_project_stubs_materialized if live_project_stubs_materialized is not None \
                                    else group.projects.list(all=True, include_subgroups=True, statistics=False, lazy=True)
                for proj_stub in projects_iterator:
                    with processed_counter_lock:
                        if debug_limit is not None and processed_counter[0] >= debug_limit:
                            group_specific_logger.info(f"Global debug limit ({debug_limit}) reached. Stopping further project submissions.")
                            break
                        processed_counter[0] += 1
                    
                    project_stub_path_with_namespace = proj_stub.path_with_namespace

                    if repos_created_after_filter_date: # Date filter already applied if live_project_stubs_materialized was used
                        if live_project_stubs_materialized is None: # Apply filter only if iterating fresh
                            visibility = proj_stub.visibility
                            is_not_public = visibility != "public"
                            if is_not_public:
                                created_at_dt = _parse_gitlab_iso_datetime_for_filter(proj_stub.created_at, group_specific_logger, project_stub_path_with_namespace, "created_at")
                                modified_at_dt = _parse_gitlab_iso_datetime_for_filter(proj_stub.last_activity_at, group_specific_logger, project_stub_path_with_namespace, "last_activity_at")
                                if not ((created_at_dt and created_at_dt >= repos_created_after_filter_date) or \
                                       (modified_at_dt and modified_at_dt >= repos_created_after_filter_date)):
                                    with processed_counter_lock: processed_counter[0] -=1
                                    skipped_by_date_filter_count += 1
                                    continue
                                else: # Log if it passes
                                    created_at_log_str = created_at_dt.strftime('%Y-%m-%d %H:%M:%S %Z') if created_at_dt else 'N/A'
                                    modified_at_log_str = modified_at_dt.strftime('%Y-%m-%d %H:%M:%S %Z') if modified_at_dt else 'N/A'
                                    log_message_parts = [f"GitLab: Non-public project '{project_stub_path_with_namespace}' included. "]
                                    if (created_at_dt and created_at_dt >= repos_created_after_filter_date): log_message_parts.append(f"Created on ({created_at_log_str}).")
                                    elif (modified_at_dt and modified_at_dt >= repos_created_after_filter_date): log_message_parts.append(f"Last modified on ({modified_at_log_str}).")
                                    group_specific_logger.info(" ".join(log_message_parts))
                    
                    current_commit_sha_for_cache = None
                    try:
                        # This is a REST call for SHA for pre-GraphQL cache check
                        # The GQL query will also fetch the latest SHA from the default branch.
                        # This REST call is kept for the pre-GraphQL cache check.
                        # Apply REST delay before this call
                        if cfg_obj:
                            rest_sha_delay = calculate_dynamic_delay(
                                base_delay_seconds=float(getattr(cfg_obj, 'GITLAB_POST_API_CALL_DELAY_SECONDS_ENV', "0.1")),
                                num_items=num_projects_in_target_for_delay_calc if num_projects_in_target_for_delay_calc > 0 else None,
                                threshold_items=int(getattr(cfg_obj, 'DYNAMIC_DELAY_THRESHOLD_REPOS_ENV', "100")),
                                scale_factor=float(getattr(cfg_obj, 'DYNAMIC_DELAY_SCALE_FACTOR_ENV', "1.5")),
                                max_delay_seconds=float(getattr(cfg_obj, 'DYNAMIC_DELAY_MAX_SECONDS_ENV', "1.0")),
                                num_workers=max_workers
                            )
                            if rest_sha_delay > 0:
                                group_specific_logger.debug(f"Applying SYNC REST SHA fetch delay: {rest_sha_delay:.2f}s")
                                time.sleep(rest_sha_delay)

                        project_for_sha = gl_for_listing.projects.get(proj_stub.id, lazy=False)
                        if project_for_sha.empty_repo:
                            group_specific_logger.info(f"Project {project_stub_path_with_namespace} is empty. Cannot get current commit SHA for caching.")
                        elif project_for_sha.default_branch:
                            commits = project_for_sha.commits.list(ref_name=project_for_sha.default_branch, per_page=1, get_all=False)
                            if commits:
                                current_commit_sha_for_cache = commits[0].id
                                group_specific_logger.debug(f"Fetched current commit SHA '{current_commit_sha_for_cache}' for {project_stub_path_with_namespace}.")
                    except GitlabGetError as e_sha_fetch:
                        group_specific_logger.warning(f"API error fetching current commit SHA for {project_stub_path_with_namespace}: {e_sha_fetch}.")
                    except Exception as e_sha_unexpected:
                        group_specific_logger.error(f"Unexpected error fetching current commit SHA for {project_stub_path_with_namespace}: {e_sha_unexpected}.", exc_info=True)

                    project_count_for_group_submitted += 1
                    future = executor.submit(
                        _process_single_gitlab_project,
                        proj_stub.id,
                        group.full_path, # Pass the parent group's full path
                        token,
                        effective_gitlab_url_for_threads,
                        ssl_verify_flag,
                        hours_per_commit,
                        cfg_obj,
                        inter_repo_adaptive_delay_per_repo,
                        dynamic_post_api_call_delay_seconds,
                        previous_scan_cache,
                        current_commit_sha_for_cache,
                        num_repos_in_target=num_projects_in_target_for_delay_calc, # Pass for labor estimator/internal delays
                        num_workers=max_workers,
                        logger_instance=group_specific_logger # Pass the adapter
                    )
                    future_to_project_name[future] = proj_stub.path_with_namespace
            
            except GitlabListError as gl_list_err:
                group_specific_logger.error(f"API error during project listing: {gl_list_err}")
            except Exception as ex_iter:
                group_specific_logger.error(f"Unexpected error during project listing: {ex_iter}.")

            for future in as_completed(future_to_project_name):
                project_name_for_log = future_to_project_name[future]
                try:
                    project_data_result = future.result()
                    if project_data_result and project_data_result.get("processing_status") != "skipped_fork":
                        processed_repo_list.append(project_data_result)
                except Exception as exc:
                    group_specific_logger.error(f"Project {project_name_for_log} generated an exception: {exc}", exc_info=True)
                    processed_repo_list.append({"name": project_name_for_log.split('/')[-1], 
                                                "organization": group.full_path, 
                                                "processing_error": f"Thread execution failed: {exc}"})

        group_specific_logger.info(f"Finished processing for {project_count_for_group_submitted} projects. Collected {len(processed_repo_list)} results.")
        if repos_created_after_filter_date and skipped_by_date_filter_count > 0:
            group_specific_logger.info(f"Skipped {skipped_by_date_filter_count} non-public projects due to date filter.")

    except GitlabAuthenticationError:
        logger.critical(f"{ANSI_RED}GitLab authentication failed for URL {effective_gitlab_url_for_threads}. Check token.{ANSI_RESET}")
    except GitlabGetError as e:
        logger.critical(f"{ANSI_RED}GitLab API error: Could not find group '{group_path}' on {effective_gitlab_url_for_threads} or other API issue: {e.error_message} (Status: {e.response_code}).{ANSI_RESET}")
    except GitlabListError as e:
        logger.critical(f"{ANSI_RED}GitLab API error listing projects for group '{group_path}' on {effective_gitlab_url_for_threads}: {e.error_message}.{ANSI_RESET}")
    except Exception as e:
        logger.critical(f"{ANSI_RED}An unexpected error occurred for GitLab group '{group_path}': {e}{ANSI_RESET}", exc_info=True)

    return processed_repo_list


if __name__ == '__main__':
    from dotenv import load_dotenv as load_dotenv_for_test
    # Assuming utils.logging_config is created and contains ContextualLogFormatter
    from utils.logging_config import ContextualLogFormatter

    load_dotenv_for_test()
    
    # Ensure basicConfig is not called if explicit handler setup is used below
    test_formatter = ContextualLogFormatter('%(asctime)s - [%(org_group)s] - %(name)s - %(levelname)s - %(message)s')

    test_gl_token = os.getenv("GITLAB_TOKEN_TEST") 
    test_gl_url_env = os.getenv("GITLAB_URL_TEST", "https://gitlab.com")
    test_group_paths_str = os.getenv("GITLAB_GROUPS_TEST", "")
    test_group_path = test_group_paths_str.split(',')[0].strip() if test_group_paths_str else None

    if not test_gl_token or is_placeholder_token(test_gl_token):
        logging.getLogger(__name__).error("Test GitLab token (GITLAB_TOKEN_TEST) not found or is a placeholder in .env.")
    elif not test_group_path:
        logging.getLogger(__name__).error("No GitLab group found in GITLAB_GROUPS_TEST in .env for testing.")
    else:
        # Setup basicConfig with the custom formatter for the test run
        root_logger_for_test = logging.getLogger()
        root_logger_for_test.handlers.clear() # Clear any default handlers
        test_handler = logging.StreamHandler()
        test_handler.setFormatter(test_formatter)
        root_logger_for_test.addHandler(test_handler)
        root_logger_for_test.setLevel(logging.INFO)

        logging.getLogger(__name__).info(f"--- Testing GitLab Connector for group: {test_group_path} on {test_gl_url_env} ---")
        counter = [0]
        counter_lock = threading.Lock()
        
        class MockCfg: # Basic mock for testing
            ADAPTIVE_DELAY_ENABLED_ENV = False
            GITLAB_URL_ENV = test_gl_url_env
            # Add other relevant cfg attributes if needed by the connector during test
            AI_ENABLED_ENV = False
            MAX_TOKENS_ENV = 1000
            AI_MAX_OUTPUT_TOKENS_ENV = 200
            AI_MODEL_NAME_ENV = "mock_model"
            AI_TEMPERATURE_ENV = 0.1


        mock_cfg_instance = MockCfg()

        repositories = fetch_repositories(
            token=test_gl_token, 
            group_path=test_group_path, 
            processed_counter=counter, 
            processed_counter_lock=counter_lock,
            debug_limit=2, # Test with a small limit
            gitlab_instance_url=test_gl_url_env,
            cfg_obj=mock_cfg_instance, # Pass the mock config
            previous_scan_output_file=None,
            hours_per_commit=0.5
        )

        if repositories:
            logging.getLogger(__name__).info(f"Successfully fetched {len(repositories)} repositories.")
            for i, repo_info in enumerate(repositories):
                logging.getLogger(__name__).info(f"--- Repository {i+1} ({repo_info.get('name')}) ---")
                logging.getLogger(__name__).info(f"  Repo ID: {repo_info.get('repo_id')}")
                logging.getLogger(__name__).info(f"  Last Commit SHA: {repo_info.get(PLATFORM_CACHE_CONFIG['gitlab']['commit_sha_field'])}")
                if "processing_error" in repo_info:
                    logging.getLogger(__name__).error(f"  Processing Error: {repo_info['processing_error']}")
        else:
            logging.getLogger(__name__).warning("No repositories fetched or an error occurred.")
        logging.getLogger(__name__).info(f"Total repositories processed according to counter: {counter[0]}")
