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
from typing import List, Optional, Dict, Any, Tuple
from datetime import timezone, datetime, timedelta
from utils.caching import load_previous_scan_data, PLATFORM_CACHE_CONFIG # Corrected import order
from utils.dateparse import get_fixed_private_filter_date # Import the consolidated utility
from utils.labor_hrs_estimator import analyze_gitlab_repo_sync # Import the labor hrs estimator
from utils.fetch_utils import (
    # These might be less needed if GQL handles retries or has different error patterns
    fetch_optional_content_with_retry,
    FETCH_ERROR_FORBIDDEN, FETCH_ERROR_NOT_FOUND, FETCH_ERROR_EMPTY_REPO_API,
    FETCH_ERROR_API_ERROR, FETCH_ERROR_UNEXPECTED
)
from utils.rate_limit_utils import get_gitlab_rate_limit_status, calculate_inter_submission_delay # New


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
    previous_scan_cache: Dict[str, Dict], # project_id_str (numeric) is key
    current_commit_sha: Optional[str], # This is from REST stub's last_activity_at commit
    logger_instance: logging.LoggerAdapter, # Accept a logger instance
    num_workers: int = 1
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
    
    current_logger = logger_instance

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

        current_logger.info(f"START _process_single_gitlab_project for {project_rest_stub.path_with_namespace} (ID: {project_stub_id or 'Unknown initially'})")

        # --- Fetch License Info via REST API ---
        # The license field is not reliably available via GraphQL on this instance.
        # Use the REST API endpoint with the 'license' flag.
        license_info_rest: Optional[Dict[str, Any]] = None # type: ignore
        try:
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
                   # repo_data_to_process["organization"] = group_full_path # Ensure org is correct
                    if cfg_obj:
                        repo_data_to_process = exemption_processor.process_repository_exemptions(
                            repo_data_to_process,
                            scm_org_for_logging=group_full_path, 
                            cfg_obj=cfg_obj,
                            default_org_identifiers=[group_full_path],
                            logger_instance=current_logger
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
                project_id=str(project_rest_stub.id), 
                token=token,
                hours_per_commit=hours_per_commit, 
                gitlab_api_url=effective_gitlab_url,
                gl_instance_for_gql=gl_instance_for_task, # Pass the client for GQL commit history
                default_branch_override=project_rest_stub.default_branch, # Pass default branch
                cfg_obj=cfg_obj, # Pass cfg_obj for its own delay calculations
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
                default_org_identifiers=[group_full_path],
                logger_instance=current_logger)
        else:
            current_logger.warning(f"cfg_obj not provided to _process_single_gitlab_project for {repo_full_name}.")
            repo_data = exemption_processor.process_repository_exemptions(
                repo_data,
                scm_org_for_logging=group_full_path, 
                cfg_obj=cfg_obj, # No cfg_obj, so no adaptive delay or other cfg-based processing
                default_org_identifiers=[group_full_path],
                logger_instance=current_logger)

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

def _get_project_stubs_and_estimate_api_calls(
    group_obj: Any, # gitlab.objects.Group
    group_full_path: str, # For logging
    fixed_private_filter_date: datetime,
    hours_per_commit: Optional[float],
    cfg_obj: Any,
    logger_instance: logging.LoggerAdapter,
    previous_scan_cache: Dict[str, Dict],
    gl_client_for_estimation: gitlab.Gitlab # Client for GQL peek and REST SHA (if needed)
) -> tuple[List[Dict[str, Any]], int]:
    """
    Internal helper to list project stubs, filter them, and estimate API calls.
    Returns a list of enriched project info dicts and the estimated API calls for them.
    """
    logger_instance.info(f"Fetching all project stubs for group '{group_full_path}'...")
    gitlab_cache_config = PLATFORM_CACHE_CONFIG["gitlab"]
    all_project_stubs_in_group = []
    try:
        project_paginator = group_obj.projects.list(all=True, include_subgroups=True, statistics=False, lazy=True)
        all_project_stubs_in_group = list(project_paginator)
        logger_instance.info(f"Fetched {len(all_project_stubs_in_group)} total project stubs for '{group_full_path}'.")
    except GitlabListError as rle_list:
        logger_instance.error(f"GitLab API error while listing projects for '{group_full_path}': {rle_list}. Cannot proceed.")
        raise
    except Exception as e_list:
        logger_instance.error(f"Error listing projects for '{group_full_path}': {e_list}. Cannot proceed.", exc_info=True)
        raise

    enriched_projects_list: List[Dict[str, Any]] = []
    api_calls_for_listing = (len(all_project_stubs_in_group) // 50) + 1 # Rough estimate
    api_calls_for_sha_checks_gql_in_estimation = 0
    api_calls_for_full_processing_estimation = 0
    skipped_by_date_filter_count = 0
    skipped_empty_repo_count = 0 # New counter for empty repos

    # --- Configuration for GQL Retries during pre-scan peek ---
    if cfg_obj and hasattr(cfg_obj, 'GITLAB_GQL_MAX_RETRIES_ENV'):
        max_gql_peek_retries = int(getattr(cfg_obj, 'GITLAB_GQL_MAX_RETRIES_ENV'))
    else:
        max_gql_peek_retries = int(os.getenv("GITLAB_GQL_MAX_RETRIES", "2"))

    if cfg_obj and hasattr(cfg_obj, 'GITLAB_GQL_INITIAL_RETRY_DELAY_ENV'):
        initial_gql_peek_delay = float(getattr(cfg_obj, 'GITLAB_GQL_INITIAL_RETRY_DELAY_ENV'))
    else:
        initial_gql_peek_delay = float(os.getenv("GITLAB_GQL_INITIAL_RETRY_DELAY", "30"))

    if cfg_obj and hasattr(cfg_obj, 'GITLAB_GQL_RETRY_BACKOFF_FACTOR_ENV'):
        gql_peek_backoff_factor = float(getattr(cfg_obj, 'GITLAB_GQL_RETRY_BACKOFF_FACTOR_ENV'))
    else:
        gql_peek_backoff_factor = float(os.getenv("GITLAB_GQL_RETRY_BACKOFF_FACTOR", "1.5"))

    MAX_INDIVIDUAL_PEEK_RETRY_DELAY_SECONDS = float(os.getenv("GITLAB_GQL_MAX_INDIVIDUAL_RETRY_DELAY", "300"))

    # Constants for full scan estimation
    API_CALLS_PER_FULL_GITLAB_GQL_SCAN_ESTIMATE = 1 # Main GQL call
    API_CALLS_FOR_GITLAB_LICENSE_REST = 1
    est_calls_labor_gitlab = getattr(cfg_obj, 'ESTIMATED_LABOR_CALLS_PER_REPO_GITLAB_ENV',
                                   getattr(cfg_obj, 'ESTIMATED_LABOR_CALLS_PER_REPO_ENV', "3")) if hours_per_commit else "0"

    for proj_stub in all_project_stubs_in_group:
        include_project = False
        if proj_stub.visibility == "public":
            include_project = True
        else: # private or internal, apply date filter
            created_at_dt = _parse_gitlab_iso_datetime_for_filter(proj_stub.created_at, logger_instance, proj_stub.path_with_namespace, "created_at")
            modified_at_dt = _parse_gitlab_iso_datetime_for_filter(proj_stub.last_activity_at, logger_instance, proj_stub.path_with_namespace, "last_activity_at")
            if (created_at_dt and created_at_dt >= fixed_private_filter_date) or \
               (modified_at_dt and modified_at_dt >= fixed_private_filter_date):
                include_project = True
            else:
                skipped_by_date_filter_count +=1
        
        # NEW: Add check for empty repository using proj_stub.empty_repo
        if include_project: # If it's still a candidate after privacy/date filters
            # Check if the repository is empty using the 'empty_repo' attribute from the REST API stub.
            if hasattr(proj_stub, 'empty_repo') and proj_stub.empty_repo:
                logger_instance.info(f"Pre-scan: GitLab project '{proj_stub.path_with_namespace}' identified as empty (empty_repo: True from REST stub). Skipping further processing for this project in estimation phase.")
                include_project = False
                skipped_empty_repo_count += 1
        
        if include_project:
            project_id_str = str(proj_stub.id)
            repo_name_for_log = proj_stub.path_with_namespace
            live_sha: Optional[str] = None
            # live_sha_date will be derived from proj_stub.last_activity_at for GitLab
            live_sha_date = _parse_gitlab_iso_datetime_for_filter(proj_stub.last_activity_at, logger_instance, repo_name_for_log, "last_activity_at_from_stub")

            # fetch_project_short_metadata_graphql now handles its own retries.
            # Pass retry parameters suitable for "peek" calls.
            peek_data = gitlab_gql.fetch_project_short_metadata_graphql(
                gl_instance=gl_client_for_estimation,
                project_full_path=proj_stub.path_with_namespace,
                logger_instance=logger_instance,
                max_retries=max_gql_peek_retries,
                initial_delay_seconds=initial_gql_peek_delay,
                backoff_factor=gql_peek_backoff_factor,
                max_individual_delay_seconds=MAX_INDIVIDUAL_PEEK_RETRY_DELAY_SECONDS
            )
            api_calls_for_sha_checks_gql_in_estimation += 1 # Count this GQL call attempt

            if peek_data and peek_data.get('id') is not None: # Check if peek was successful (ID is present)
                live_sha = peek_data.get('lastCommitSHA')
                # The 'id' from peek_data is the integer project ID.
                # We already have project_id_str from proj_stub.id, which should match.
                # If peek_data['id'] is different, it might indicate an issue, but we'll trust proj_stub.id for consistency.
                if str(peek_data.get('id')) != project_id_str:
                    logger_instance.warning(
                        f"Pre-scan GQL Peek for {repo_name_for_log}: Mismatch between stub ID ({project_id_str}) and GQL peek ID ({peek_data.get('id')}). "
                        "Using stub ID for cache key."
                    )
            elif peek_data and peek_data.get('error'):
                logger_instance.warning(
                    f"Pre-scan GQL Peek for {repo_name_for_log} failed after retries: {peek_data.get('error')}. "
                    "Proceeding without live SHA from GQL."
                )
            else: # GQL peek didn't return valid data (e.g., project not found via GQL, though stub exists)
                logger_instance.warning(
                    f"Pre-scan GQL Peek for {repo_name_for_log} did not return valid data. "
                    "Proceeding without live SHA from GQL."
                )


            repo_visibility = proj_stub.visibility # "public", "internal", "private"
            is_cached = project_id_str in previous_scan_cache if project_id_str else False
            is_changed = False
            if is_cached:
                cached_sha = previous_scan_cache[project_id_str].get(gitlab_cache_config["commit_sha_field"])
                is_changed = (cached_sha != live_sha)
            else: # Not cached
                # if the repo was changed after the establisged date (June 21, 2025) then is_changed = true and is_desired_for_processing = true
                if repo_visibility != "public": # For private or internal repos
                    if live_sha_date: # Use date from proj_stub.last_activity_at
                        is_changed = live_sha_date >= fixed_private_filter_date
                    else:
                        is_changed = True # If live_sha_date is None or not available, consider the repo changed.

            is_desired_for_processing = (repo_visibility == "public") or is_changed or is_cached

            enriched_project_info = {
                "repo_stub_obj": proj_stub, # Original stub
                "repo_id_str": project_id_str,
                "repo_name_for_log": repo_name_for_log,
                "live_sha": live_sha,
                "live_sha_date": live_sha_date,
                "visibility": repo_visibility,
                "is_cached": is_cached,
                "is_changed": is_changed,
                "is_desired_for_processing": is_desired_for_processing,
            }
            enriched_projects_list.append(enriched_project_info)

            if is_desired_for_processing and not (is_cached and not is_changed):
                api_calls_for_full_processing_estimation += API_CALLS_PER_FULL_GITLAB_GQL_SCAN_ESTIMATE + API_CALLS_FOR_GITLAB_LICENSE_REST
                if hours_per_commit:
                    api_calls_for_full_processing_estimation += int(est_calls_labor_gitlab)

    total_estimated_calls = api_calls_for_listing + api_calls_for_sha_checks_gql_in_estimation + api_calls_for_full_processing_estimation
    logger_instance.info(f"Identified {len(enriched_projects_list)} projects to potentially process for '{group_full_path}'. Estimated API calls for this target: {total_estimated_calls}")
    if skipped_by_date_filter_count > 0:
        logger_instance.info(f"Skipped {skipped_by_date_filter_count} non-public projects from '{group_full_path}' due to fixed date filter ({fixed_private_filter_date.strftime('%Y-%m-%d')}).")
    if skipped_empty_repo_count > 0:
        logger_instance.info(f"Skipped {skipped_empty_repo_count} empty projects from '{group_full_path}' during pre-scan estimation.")
    return enriched_projects_list, total_estimated_calls


def fetch_repositories(
    token: Optional[str], 
    group_path: str, 
    processed_counter: List[int], 
    processed_counter_lock: threading.Lock,
    logger_instance: logging.Logger,
    debug_limit: int | None = None,
    gitlab_instance_url: str | None = None,
    hours_per_commit: Optional[float] = None,
    max_workers: int = 5, 
    cfg_obj: Optional[Any] = None,
    previous_scan_output_file: Optional[str] = None,
    pre_fetched_enriched_repos: Optional[List[Dict[str, Any]]] = None,
    global_inter_submission_delay: Optional[float] = None
) -> list[dict]:
    """Fetches repository (project) details from a specific GitLab group."""
    # Ensure logger_instance is provided
    current_logger = logger_instance # Directly use the passed-in adapter
    effective_gitlab_url = _get_effective_gitlab_url(gitlab_instance_url, cfg_obj, current_logger) # Pass it along
    
    current_logger.info(f"Attempting to fetch repositories for GitLab group: {ANSI_YELLOW}{group_path} on {effective_gitlab_url}{ANSI_RESET} (max_workers: {max_workers})")

    if is_placeholder_token(token):
        current_logger.error("GitLab token is a placeholder or missing. Cannot fetch repositories.")
        return []
    if not group_path:
        current_logger.warning("GitLab group path not provided. Skipping GitLab scan.")
        return []
    
    fixed_private_filter_date = get_fixed_private_filter_date(cfg_obj, current_logger)
    ssl_verify_flag = os.getenv("DISABLE_SSL_VERIFICATION", "false").lower() != "true"
    if not ssl_verify_flag:
        current_logger.warning(f"{ANSI_RED}SECURITY WARNING: SSL certificate verification is DISABLED for GitLab connections.{ANSI_RESET}")

    previous_scan_cache: Dict[str, Dict] = {}
    if previous_scan_output_file:
        current_logger.info(f"Attempting to load previous GitLab scan data for group '{group_path}' from: {previous_scan_output_file}")
        loaded_cache = load_previous_scan_data(previous_scan_output_file, "gitlab")
        if isinstance(loaded_cache, dict): previous_scan_cache = loaded_cache
        else: current_logger.warning(f"CACHE: load_previous_scan_data did not return a dict for {previous_scan_output_file}. Cache will be empty.")

    processed_repo_list: List[Dict[str, Any]] = []
    gl_for_listing, group_obj = _initialize_gitlab_client_and_get_group(effective_gitlab_url, token, group_path, ssl_verify_flag, current_logger)
    if not group_obj:
        return [] # Error already logged

    if pre_fetched_enriched_repos is not None:
        enriched_repo_list = pre_fetched_enriched_repos
        current_logger.info(f"Using pre-fetched enriched repository list for GitLab group '{group_path}'.")
    else:
        current_logger.warning(f"No pre-fetched enriched repository list for GitLab group '{group_path}'. Fetching now (less optimal).")
        try:
            enriched_repo_list, _ = _get_project_stubs_and_estimate_api_calls(
                group_obj, group_path, fixed_private_filter_date, hours_per_commit, 
                cfg_obj, current_logger, previous_scan_cache, gl_for_listing # gl_for_listing can be used for GQL peeks
            )
        except Exception: # Errors from listing/estimation
            return []

    if not enriched_repo_list:
        current_logger.info(f"No projects to process for '{group_path}' after filtering. Skipping.")
        return []

    # Determine inter_submission_delay
    if global_inter_submission_delay is not None:
        inter_submission_delay = global_inter_submission_delay
        current_logger.info(f"Using globally calculated inter-submission delay: {inter_submission_delay:.3f}s for GitLab group '{group_path}'.")
    else:
        # Fallback if global delay not provided (less optimal)
        current_logger.warning(f"Global inter-submission delay not provided for GitLab group '{group_path}'. Calculating locally (less optimal).")
        current_rate_limit_status = get_gitlab_rate_limit_status(gl_for_listing, current_logger)
        if not current_rate_limit_status:
            current_logger.error(f"Could not determine current rate limit for '{group_path}'. Aborting target.")
            return []
        
        _, estimated_api_calls_for_current_target_fallback = _get_project_stubs_and_estimate_api_calls(
             group_obj, group_path, fixed_private_filter_date, hours_per_commit, 
             cfg_obj, current_logger, previous_scan_cache, gl_for_listing
        )
        inter_submission_delay = calculate_inter_submission_delay(
            rate_limit_status=current_rate_limit_status,
            estimated_api_calls_for_target=estimated_api_calls_for_current_target_fallback,
            num_workers=max_workers,
            safety_factor=getattr(cfg_obj, 'API_SAFETY_FACTOR_ENV', 0.8),
            min_delay_seconds=getattr(cfg_obj, 'MIN_INTER_REPO_DELAY_SECONDS_ENV', 0.1),
            max_delay_seconds=getattr(cfg_obj, 'MAX_INTER_REPO_DELAY_SECONDS_ENV', 30.0)
        )

    project_count_for_group_submitted = 0
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_project_name = {}
        try:
            for enriched_repo in enriched_repo_list:
                with processed_counter_lock:
                    if debug_limit is not None and processed_counter[0] >= debug_limit:
                        current_logger.info(f"Global debug limit ({debug_limit}) reached. Stopping further project submissions.")
                        break
                    processed_counter[0] += 1
                
                proj_stub_obj = enriched_repo["repo_stub_obj"]
                repo_name_for_log = enriched_repo["repo_name_for_log"]

                if not enriched_repo['is_desired_for_processing']:
                    current_logger.info(f"Skipping {repo_name_for_log} as it's not desired for processing based on pre-scan.")
                    continue

                actual_delay_this_submission = 0.0
                log_message_suffix = ""

                if enriched_repo['is_cached'] and not enriched_repo['is_changed']:
                    actual_delay_this_submission = getattr(cfg_obj, 'CACHE_HIT_SUBMISSION_DELAY_SECONDS_ENV', 0.05)
                    log_message_suffix = f"CACHE HIT (pre-scan): Using minimal submission delay: {actual_delay_this_submission:.3f}s"
                else: # Needs full scan
                    actual_delay_this_submission = inter_submission_delay
                    log_message_suffix = f"FULL SCAN needed: Using standard submission delay: {actual_delay_this_submission:.3f}s"
                    # Potential "peek for NEXT repo" logic could go here

                current_logger.info(f"Submission delay for {repo_name_for_log}: {log_message_suffix}", extra={'org_group': group_path})
                if actual_delay_this_submission > 0:
                    time.sleep(actual_delay_this_submission)
                
                # Pass live SHA from pre-scan to worker. Worker uses this for its cache check.
                # The _get_current_commit_sha_for_cache call here is redundant if live_sha from pre-scan is reliable.
                # For GitLab, the GQL peek provides lastCommitSHA.
                current_commit_sha_for_cache = enriched_repo['live_sha'] 

                project_count_for_group_submitted += 1
                future = executor.submit(
                    _process_single_gitlab_project,
                    proj_stub_obj.id, group_obj.full_path, token, # Pass original stub ID
                    effective_gitlab_url, ssl_verify_flag,
                    hours_per_commit, cfg_obj, previous_scan_cache,
                    current_commit_sha_for_cache,
                    num_workers=max_workers, logger_instance=current_logger # Pass the logger
                )
                future_to_project_name[future] = repo_name_for_log
        
        except GitlabListError as gl_list_err: current_logger.error(f"API error during project iteration: {gl_list_err}") # Should be caught by _get_project_stubs...
        except Exception as ex_iter: current_logger.error(f"Unexpected error during project iteration: {ex_iter}.")

        for future in as_completed(future_to_project_name):
            project_name_for_log = future_to_project_name[future]
            try:
                project_data_result = future.result()
                if project_data_result and project_data_result.get("processing_status") != "skipped_fork":
                    processed_repo_list.append(project_data_result)
            except Exception as exc:
                current_logger.error(f"Project {project_name_for_log} generated an exception: {exc}", exc_info=True)
                processed_repo_list.append({"name": project_name_for_log.split('/')[-1], 
                                            "organization": group_obj.full_path, 
                                            "processing_error": f"Thread execution failed: {exc}"})

    current_logger.info(f"Finished processing for {project_count_for_group_submitted} projects. Collected {len(processed_repo_list)} results.")
    return processed_repo_list

def _get_effective_gitlab_url(gitlab_instance_url: Optional[str], cfg_obj: Any, logger_instance: logging.LoggerAdapter) -> str:
    """Determines the effective GitLab URL to use."""
    effective_url = gitlab_instance_url
    if not effective_url:
        effective_url = cfg_obj.GITLAB_URL_ENV if cfg_obj and hasattr(cfg_obj, 'GITLAB_URL_ENV') else "https://gitlab.com"
        if effective_url == "https://gitlab.com": # Only log warning if it defaulted to public
            logger_instance.warning(f"No GitLab instance URL provided or in cfg_obj.GITLAB_URL_ENV. Using default: {effective_url}")
    return effective_url.strip('/')

def _initialize_gitlab_client_and_get_group(
    effective_gitlab_url: str, token: Optional[str], group_path: str, ssl_verify: bool, logger_instance: logging.LoggerAdapter
) -> tuple[Optional[gitlab.Gitlab], Optional[Any]]:
    """Initializes GitLab client and fetches the group object."""
    try:
        gl_client = gitlab.Gitlab(effective_gitlab_url, private_token=token, timeout=30, ssl_verify=ssl_verify)
        gl_client.auth() 
        logger_instance.info(f"Successfully connected and authenticated to GitLab instance for listing: {effective_gitlab_url}")
        group_obj = gl_client.groups.get(group_path, lazy=False)
        logger_instance.info(f"Successfully found GitLab group: {group_obj.full_path} (ID: {group_obj.id})")
        return gl_client, group_obj
    except GitlabAuthenticationError:
        logger_instance.critical(f"{ANSI_RED}GitLab authentication failed for URL {effective_gitlab_url}. Check token.{ANSI_RESET}")
    except GitlabGetError as e:
        logger_instance.critical(f"{ANSI_RED}GitLab API error: Could not find group '{group_path}' on {effective_gitlab_url} or other API issue: {e.error_message} (Status: {e.response_code}).{ANSI_RESET}")
    except GitlabListError as e:
        logger_instance.critical(f"{ANSI_RED}GitLab API error listing projects for group '{group_path}' on {effective_gitlab_url}: {e.error_message}.{ANSI_RESET}")
    except Exception as e:
        logger_instance.critical(f"{ANSI_RED}An unexpected error occurred for GitLab group '{group_path}': {e}{ANSI_RESET}", exc_info=True)
    return None, None

def _get_current_commit_sha_for_cache(gl_instance: gitlab.Gitlab, proj_stub: Any, logger_instance: logging.LoggerAdapter) -> Optional[str]:
    """Fetches the current commit SHA for a project stub for caching purposes."""
    try:
        project_for_sha = gl_instance.projects.get(proj_stub.id, lazy=False)
        if project_for_sha.empty_repo:
            logger_instance.info(f"Project {proj_stub.path_with_namespace} is empty. Cannot get current commit SHA for caching.")
        elif project_for_sha.default_branch:
            commits = project_for_sha.commits.list(ref_name=project_for_sha.default_branch, per_page=1, get_all=False)
            if commits:
                logger_instance.debug(f"Fetched current commit SHA '{commits[0].id}' for {proj_stub.path_with_namespace}.")
                return commits[0].id
    except GitlabGetError as e_sha_fetch:
        logger_instance.warning(f"API error fetching current commit SHA for {proj_stub.path_with_namespace}: {e_sha_fetch}.")
    except Exception as e_sha_unexpected:
        logger_instance.error(f"Unexpected error fetching current commit SHA for {proj_stub.path_with_namespace}: {e_sha_unexpected}.", exc_info=True)
    return None

def estimate_api_calls_for_group(
    token: Optional[str],
    group_path: str,
    gitlab_instance_url: Optional[str],
    cfg_obj: Any,
    logger_instance: logging.LoggerAdapter # Made non-optional
) -> Tuple[List[Dict[str, Any]], int]: # Returns enriched list and estimate
    """
    Estimates the number of API calls required to process a GitLab group.
    This is used by the orchestrator for pre-scan estimation.
    """
    current_logger = logger_instance # Directly use the passed-in adapter
    current_logger.info(f"Estimating API calls for GitLab group: {group_path}")

    if is_placeholder_token(token):
        current_logger.error("GitLab token is a placeholder or missing. Cannot estimate.")
        return [], 0
    if not group_path:
        current_logger.warning("GitLab group path not provided. Cannot estimate.")
        return [], 0

    effective_gitlab_url = _get_effective_gitlab_url(gitlab_instance_url, cfg_obj, current_logger)
    fixed_private_filter_date = get_fixed_private_filter_date(cfg_obj, current_logger)
    ssl_verify_flag = os.getenv("DISABLE_SSL_VERIFICATION", "false").lower() != "true"

    gl_client_for_est, group_obj = _initialize_gitlab_client_and_get_group(effective_gitlab_url, token, group_path, ssl_verify_flag, current_logger)
    if not group_obj:
        return [], 0

    # Load cache for estimation
    previous_intermediate_filepath = os.path.join(getattr(cfg_obj, 'OUTPUT_DIR', '.'), f"intermediate_gitlab_{group_path.replace('/', '_')}.json")
    previous_scan_cache_for_estimation = load_previous_scan_data(previous_intermediate_filepath, "gitlab")

    try:
        hpc_val = None
        if hasattr(cfg_obj, 'HOURS_PER_COMMIT_ENV') and cfg_obj.HOURS_PER_COMMIT_ENV is not None:
            try: hpc_val = float(cfg_obj.HOURS_PER_COMMIT_ENV)
            except ValueError: pass
        enriched_list, estimated_calls = _get_project_stubs_and_estimate_api_calls(
            group_obj, group_path, fixed_private_filter_date, hpc_val, 
            cfg_obj, current_logger, previous_scan_cache_for_estimation, gl_client_for_est
        )
        return enriched_list, estimated_calls
    except Exception: # Catch errors from _get_project_stubs_and_estimate_api_calls
        return [], 0