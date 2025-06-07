# clients/github_connector.py
"""
GitHub Connector for Share IT Act Repository Scanning Tool.

This module is responsible for fetching repository data from GitHub,
including metadata, README content, CODEOWNERS files, topics, and tags.
It interacts with the GitHub API, primarily using GraphQL for detailed
repository data and PyGithub for listing repositories.
"""

import os
import logging
import time
import threading # For locks
from concurrent.futures import ThreadPoolExecutor, as_completed # type: ignore
from typing import List, Dict, Optional, Any, Tuple
from datetime import timezone, datetime, timedelta

from utils.caching import load_previous_scan_data, PLATFORM_CACHE_CONFIG
from utils.rate_limit_utils import get_github_rate_limit_status, calculate_inter_submission_delay # New
from utils.dateparse import get_fixed_private_filter_date # Import the consolidated utility
from utils.labor_hrs_estimator import analyze_github_repo_sync

from github import Github, GithubException, UnknownObjectException, RateLimitExceededException

# Import the new GraphQL client
from .graphql_clients import github_gql

# ANSI escape codes for coloring output
ANSI_YELLOW = "\x1b[33;1m"
ANSI_RED = "\x1b[31;1m"
ANSI_RESET = "\x1b[0m"

# Attempt to import the exemption processor
try:
    from utils import exemption_processor
except ImportError:
    logging.getLogger(__name__).error(
        "Failed to import exemption_processor from utils. "
        "Exemption processing will be skipped by the GitHub connector (using mock)."
    )
    class MockExemptionProcessor:
        def process_repository_exemptions(self, repo_data: Dict[str, Any], default_org_identifiers: Optional[List[str]] = None, **kwargs: Any) -> Dict[str, Any]:
            repo_data.setdefault('_status_from_readme', None)
            repo_data.setdefault('_private_contact_emails', [])
            repo_data.setdefault('contact', {})
            repo_data.setdefault('permissions', {"usageType": "openSource"})
            repo_data.pop('readme_content', None)
            repo_data.pop('_codeowners_content', None)
            return repo_data
    exemption_processor = MockExemptionProcessor()

logger = logging.getLogger(__name__) # Renamed from special_logger
PLACEHOLDER_GITHUB_TOKEN = "YOUR_GITHUB_PAT"

def is_placeholder_token(token: Optional[str]) -> bool:
    """Checks if the GitHub token is missing or a known placeholder."""
    return not token or token == PLACEHOLDER_GITHUB_TOKEN

def _process_single_github_repository(
    repo_stub, # Can be a PyGithub Repository stub or full object
    org_name: str,
    token: Optional[str],
    github_instance_url: Optional[str],
    hours_per_commit: Optional[float],
    cfg_obj: Optional[Any], # Made Optional[Any] explicit based on usage
    graphql_endpoint_url_for_client: Optional[str], # Pass URL to create client
    previous_scan_cache: Dict[str, Dict],
    num_repos_in_target: Optional[int], # Added to accept this parameter
    logger_instance: Optional[logging.Logger] = None,
    num_workers: int = 1,
    # New optional parameters for peeked data (now from pre-scan)
    live_commit_sha_from_prescan: Optional[str] = None,
    live_repo_id_from_prescan: Optional[str] = None
) -> Dict[str, Any]:
    """
    Processes a single GitHub repository using GraphQL to extract its metadata.
    """
    repo_name_for_gql = repo_stub.name # repo_stub is the original stub object
    repo_full_name_logging = f"{org_name}/{repo_name_for_gql}"
    repo_data: Dict[str, Any] = {"name": repo_name_for_gql, "organization": org_name}
    github_cache_config = PLATFORM_CACHE_CONFIG["github"]
    
    repo_id_str = str(repo_stub.id) if hasattr(repo_stub, 'id') and repo_stub.id else None

    # Use passed-in logger; if None, create one (though it should always be passed)
    current_logger = logger_instance # Directly use the passed-in adapter
    current_logger.info(f"START _process_single_github_repository for {repo_full_name_logging} (Stub ID: {repo_id_str or 'Unknown'})")

    try:
        # --- Early Cache Check using Pre-scanned Live SHA ---
        # live_repo_id_from_prescan is the definitive ID from the GQL peek during pre-scan.
        # repo_id_str is from the PyGithub stub, which should match.
        if live_commit_sha_from_prescan and live_repo_id_from_prescan:
            cached_repo_entry = previous_scan_cache.get(live_repo_id_from_prescan)
            if cached_repo_entry:
                cached_commit_sha_from_main_cache = cached_repo_entry.get(github_cache_config["commit_sha_field"])
                if cached_commit_sha_from_main_cache and live_commit_sha_from_prescan == cached_commit_sha_from_main_cache:
                    current_logger.info(f"CACHE HIT (via pre-scan SHA): GitHub repo '{repo_full_name_logging}' (ID: {live_repo_id_from_prescan}). Using cached data.")
                    repo_data_to_process = cached_repo_entry.copy()
                    repo_data_to_process[github_cache_config["commit_sha_field"]] = live_commit_sha_from_prescan
                    repo_data_to_process["repo_id"] = int(live_repo_id_from_prescan) if live_repo_id_from_prescan.isdigit() else None
                    if cfg_obj:
                        repo_data_to_process = exemption_processor.process_repository_exemptions(
                            repo_data_to_process, scm_org_for_logging=org_name, cfg_obj=cfg_obj, default_org_identifiers=[org_name],
                            logger_instance=current_logger )
                    return repo_data_to_process

        client_for_this_task = github_gql.get_github_gql_client(token, graphql_endpoint_url_for_client)
        if not client_for_this_task:
            current_logger.error(f"Failed to create GraphQL client for {repo_full_name_logging}. Skipping.")
            repo_data["processing_error"] = "GraphQL client creation failed"
            return repo_data

        # --- Configuration for GQL Retries ---
        if cfg_obj and hasattr(cfg_obj, 'GITHUB_GQL_MAX_RETRIES_ENV'):
            max_gql_retries_val = getattr(cfg_obj, 'GITHUB_GQL_MAX_RETRIES_ENV')
        else:
            max_gql_retries_val = os.getenv("GITHUB_GQL_MAX_RETRIES", "3")
        MAX_GQL_RATE_LIMIT_RETRIES = int(max_gql_retries_val)

        if cfg_obj and hasattr(cfg_obj, 'GITHUB_GQL_INITIAL_RETRY_DELAY_ENV'):
            initial_gql_delay_val = getattr(cfg_obj, 'GITHUB_GQL_INITIAL_RETRY_DELAY_ENV')
        else:
            initial_gql_delay_val = os.getenv("GITHUB_GQL_INITIAL_RETRY_DELAY", "60")
        INITIAL_GQL_RETRY_DELAY_SECONDS = float(initial_gql_delay_val)

        if cfg_obj and hasattr(cfg_obj, 'GITHUB_GQL_RETRY_BACKOFF_FACTOR_ENV'):
            gql_backoff_val = getattr(cfg_obj, 'GITHUB_GQL_RETRY_BACKOFF_FACTOR_ENV')
        else:
            gql_backoff_val = os.getenv("GITHUB_GQL_RETRY_BACKOFF_FACTOR", "2")
        GQL_RETRY_BACKOFF_FACTOR = float(gql_backoff_val)

        if cfg_obj and hasattr(cfg_obj, 'GITHUB_GQL_MAX_INDIVIDUAL_RETRY_DELAY_ENV'):
            max_individual_delay_val = getattr(cfg_obj, 'GITHUB_GQL_MAX_INDIVIDUAL_RETRY_DELAY_ENV')
        else:
            max_individual_delay_val = os.getenv("GITHUB_GQL_MAX_INDIVIDUAL_RETRY_DELAY", "900")
        MAX_INDIVIDUAL_RETRY_DELAY_SECONDS = float(max_individual_delay_val)
        # --- End Configuration for GQL Retries ---

        gql_data = None
        current_logger.debug(f"Attempting GQL data fetch for {repo_full_name_logging}")
        # The fetch_repository_details_graphql function now handles its own retries.
        # The connector's retry loop for this specific call is removed.
        # We pass the retry configuration from the connector to the GQL function.
        try:
            gql_data = github_gql.fetch_repository_details_graphql(
                client=client_for_this_task,
                owner=org_name,
                repo_name=repo_name_for_gql,
                logger_instance=current_logger, # Pass the logger
                max_retries=MAX_GQL_RATE_LIMIT_RETRIES,
                initial_delay_seconds=INITIAL_GQL_RETRY_DELAY_SECONDS,
                backoff_factor=GQL_RETRY_BACKOFF_FACTOR,
                max_individual_delay_seconds=MAX_INDIVIDUAL_RETRY_DELAY_SECONDS
            )
            current_logger.debug(f"GQL fetch call SUCCEEDED for {repo_full_name_logging}")
        except github_gql.TransportQueryError as gql_final_err:
            # This error is raised if fetch_repository_details_graphql failed after its internal retries,
            # or if it was a non-rate-limit GQL error.
            current_logger.error(
                f"GraphQL query ultimately failed for {repo_full_name_logging}: {gql_final_err.errors}"
            )
            repo_data["processing_error"] = f"GraphQL Query Error: {gql_final_err.errors}"
            # Let it fall through to the main error handling at the end of _process_single_github_repository
        except Exception as e_gql_other:
            current_logger.error(
                f"Unexpected error from GraphQL fetch for {repo_full_name_logging}: {e_gql_other}", exc_info=True
            )
            repo_data["processing_error"] = f"Unexpected GQL Client Error: {e_gql_other}"
            # Let it fall through

        current_logger.debug(f"GQL data fetch COMPLETED for {repo_full_name_logging}. gql_data is None: {gql_data is None}")

        if gql_data is None: # If GQL fetch failed after retries
            current_logger.error(f"Failed to fetch GraphQL data for {repo_full_name_logging} (gql_data is None after retry loop and no processing_error set). Skipping.")
            if "processing_error" not in repo_data: # Ensure error is set if not already by retry loop
                repo_data["processing_error"] = "GraphQL data fetch failed (post-retry)"
            return repo_data

        # Use GQL's databaseId as the definitive ID if available
        if gql_data.get("databaseId"):
            repo_id_str = str(gql_data["databaseId"])
        elif gql_data.get("databaseId") and repo_id_str and str(gql_data["databaseId"]) != repo_id_str :
            current_logger.warning(f"Repo ID mismatch for {repo_full_name_logging}: stub ID {repo_id_str}, GQL databaseId {gql_data['databaseId']}. Using GQL ID for processing.")
            repo_id_str = str(gql_data["databaseId"])
        
        repo_data["repo_id"] = int(repo_id_str) if repo_id_str and repo_id_str.isdigit() else None

        # This cache check is redundant if the early cache check (using pre-scanned SHA) passed.
        # However, if pre-scanned SHA was not available or didn't lead to a hit, this is a fallback.
        gql_current_commit_sha = None
        if gql_data.get("defaultBranchRef") and gql_data["defaultBranchRef"].get("target"):
            gql_current_commit_sha = gql_data["defaultBranchRef"]["target"].get("oid")

        current_logger.info(f"CACHE MISS or no prior SHA (post-GraphQL): Processing full data for {gql_data.get('nameWithOwner', repo_full_name_logging)}.")

        repo_full_name_logging = gql_data.get("nameWithOwner", repo_full_name_logging) 

        if gql_data.get("isFork"):
            current_logger.info(f"Skipping forked repository: {repo_full_name_logging}")
            repo_data["processing_status"] = "skipped_fork"
            return repo_data

        repo_data['_is_empty_repo'] = False
        if gql_data.get("diskUsage", 0) == 0 and not gql_data.get("defaultBranchRef"):
            current_logger.info(f"Repository {repo_full_name_logging} appears empty based on GraphQL (diskUsage: 0, no defaultBranchRef).")
            repo_data['_is_empty_repo'] = True
            repo_data.update({
                "description": gql_data.get("description") or "",
                "repositoryURL": gql_data.get("url"), 
                "vcs": "git", 
                "repositoryVisibility": gql_data.get("visibility", "private").lower(),
                "status": "development", "version": "N/A", "laborHours": 0,
                "languages": [], "tags": [],
                "date": {
                    "created": datetime.fromisoformat(gql_data["createdAt"].replace('Z', '+00:00')).isoformat() if gql_data.get("createdAt") else None,
                    "lastModified": datetime.fromisoformat(gql_data["pushedAt"].replace('Z', '+00:00')).isoformat() if gql_data.get("pushedAt") else None,
                },
                "permissions": {"usageType": "openSource", "exemptionText": None, "licenses": []}, 
                "contact": {}, 
                "archived": gql_data.get("isArchived", False) 
            })
            if gql_current_commit_sha:
                 repo_data[github_cache_config["commit_sha_field"]] = gql_current_commit_sha
            if cfg_obj:
                repo_data = exemption_processor.process_repository_exemptions(
                    repo_data,
                    scm_org_for_logging=org_name, 
                    cfg_obj=cfg_obj,
                    default_org_identifiers=[org_name],
                    logger_instance=current_logger)
            return repo_data


        created_at_str = gql_data.get("createdAt")
        pushed_at_str = gql_data.get("pushedAt")

        created_at_dt = datetime.fromisoformat(created_at_str.replace('Z', '+00:00')).replace(tzinfo=timezone.utc) if created_at_str else None
        pushed_at_dt = datetime.fromisoformat(pushed_at_str.replace('Z', '+00:00')).replace(tzinfo=timezone.utc) if pushed_at_str else None

        repo_visibility = gql_data.get("visibility", "private").lower()
        if repo_visibility not in ["public", "private", "internal"]:
            repo_visibility = "private"

        all_languages_list = [edge["node"]["name"] for edge in gql_data.get("languages", {}).get("edges", []) if edge and edge.get("node")]

        licenses_list = []
        license_info = gql_data.get("licenseInfo")
        if license_info and license_info.get("spdxId") and license_info["spdxId"].lower() != "noassertion":
            license_entry = {"spdxID": license_info["spdxId"]}
            if license_info.get("name"): license_entry["name"] = license_info["name"]
            if license_info.get("url"): license_entry["URL"] = license_info["url"]
            licenses_list.append(license_entry)

        readme_content_str: Optional[str] = None
        readme_html_url: Optional[str] = None
        default_branch_name = gql_data.get("defaultBranchRef", {}).get("name")

        for idx, readme_path_key in enumerate(github_gql.COMMON_README_PATHS): 
            # Sanitize the readme_path_key to match the alias generated in github_gql.py
            alias_name_for_readme = readme_path_key.replace('.', '_').replace('/', '_')
            file_data = gql_data.get(f"file_{alias_name_for_readme}_{idx}")
            if file_data and file_data.get("text") is not None:
                readme_content_str = file_data["text"]
                if default_branch_name and gql_data.get("url"):
                    readme_html_url = f"{gql_data['url']}/blob/{default_branch_name}/{readme_path_key.lstrip('/')}"
                current_logger.debug(f"Found README '{readme_path_key}' for {repo_full_name_logging} via GraphQL.")
                break
        if not readme_content_str: current_logger.debug(f"No common README file found for {repo_full_name_logging} via GraphQL.")

        codeowners_content_str: Optional[str] = None
        for idx, codeowners_path_key in enumerate(github_gql.COMMON_CODEOWNERS_PATHS): 
            alias_name_for_codeowners = codeowners_path_key.replace('.', '_').replace('/', '_')
            file_data = gql_data.get(f"file_{alias_name_for_codeowners}_{idx}")
            if file_data and file_data.get("text") is not None:
                codeowners_content_str = file_data["text"]
                current_logger.debug(f"Found CODEOWNERS '{codeowners_path_key}' for {repo_full_name_logging} via GraphQL.")
                break
        if not codeowners_content_str: current_logger.debug(f"No CODEOWNERS file found for {repo_full_name_logging} via GraphQL.")

        repo_topics = [node["topic"]["name"] for node in gql_data.get("repositoryTopics", {}).get("nodes", []) if node and node.get("topic")]
        repo_git_tags = [node["name"].replace("refs/tags/", "") for node in gql_data.get("tags", {}).get("nodes", []) if node and node.get("name")]

        repo_data.update({
            "description": gql_data.get("description") or "",
            "repositoryURL": gql_data.get("url"), 
            "homepageURL": gql_data.get("homepageUrl") or "", 
            "downloadURL": None,
            "readme_url": readme_html_url,
            "vcs": "git",
            "repositoryVisibility": repo_visibility,
            "status": "development", "version": "N/A", "laborHours": 0,
            "languages": all_languages_list,
            "tags": repo_topics, 
            "date": {
                "created": created_at_dt.isoformat() if created_at_dt else None,
                "lastModified": pushed_at_dt.isoformat() if pushed_at_dt else None,
            },
            "permissions": {"usageType": None, "exemptionText": None, "licenses": licenses_list}, 
            "contact": {}, "contractNumber": None, 
            "readme_content": readme_content_str,
            "_codeowners_content": codeowners_content_str,
            "_api_tags": repo_git_tags, 
            "archived": gql_data.get("isArchived", False),
        })
        repo_data.setdefault('_is_empty_repo', False)
        if gql_current_commit_sha:
            repo_data[github_cache_config["commit_sha_field"]] = gql_current_commit_sha

        if hours_per_commit is not None:
            current_logger.debug(f"START labor hours estimation for {repo_full_name_logging}")
            actual_default_branch_name_for_commits = None
            if gql_data.get("defaultBranchRef") and gql_data["defaultBranchRef"].get("name"):
                actual_default_branch_name_for_commits = gql_data["defaultBranchRef"]["name"]

            try:
                labor_df = analyze_github_repo_sync(
                    owner=org_name, repo=repo_name_for_gql, token=token,
                    hours_per_commit=hours_per_commit,
                    github_api_url=github_instance_url or "https://api.github.com", 
                    default_branch_override=actual_default_branch_name_for_commits, 
                    cfg_obj=cfg_obj,
                    num_repos_in_target=num_repos_in_target,
                    is_empty_repo=repo_data.get('_is_empty_repo', False),
                    number_of_workers=num_workers,
                    logger_instance=current_logger
                )
                if not labor_df.empty:
                    repo_data["laborHours"] = round(float(labor_df["EstimatedHours"].sum()), 2)
                    current_logger.info(f"Estimated labor hours for {repo_full_name_logging}: {repo_data['laborHours']}")
                else:
                    repo_data["laborHours"] = 0.0
            except Exception as e_lh:
                current_logger.warning(f"Could not estimate labor hours for {repo_full_name_logging}: {e_lh}", exc_info=True)
                repo_data["laborHours"] = 0.0
            current_logger.debug(f"END labor hours estimation for {repo_full_name_logging}")

        if cfg_obj:
            current_logger.debug(f"START exemption processing for {repo_full_name_logging}")
            # Log the presence or absence of README content being passed for exemption processing
            readme_status_for_log = "Present" if repo_data.get("readme_content") else "Absent/Empty"
            current_logger.debug(f"readme_content for exemption processing is {readme_status_for_log} for {repo_full_name_logging}")
            repo_data = exemption_processor.process_repository_exemptions(
                repo_data,
                scm_org_for_logging=org_name,
                cfg_obj=cfg_obj, 
                default_org_identifiers=[org_name],
                logger_instance=current_logger )
        else: 
            current_logger.warning(f"cfg_obj not provided to _process_single_github_repository for {repo_full_name_logging}.")
            current_logger.debug(f"START exemption processing (no cfg_obj) for {repo_full_name_logging}")
            repo_data = exemption_processor.process_repository_exemptions(
                repo_data,
                scm_org_for_logging=org_name,
                cfg_obj=cfg_obj, 
                default_org_identifiers=[org_name],
                logger_instance=current_logger
            )
        current_logger.info(f"END _process_single_github_repository for {repo_full_name_logging} (Success)")
        return repo_data

    except RateLimitExceededException as rle_repo: 
        current_logger.error(f"GitHub API (PyGithub context) rate limit exceeded processing repo {repo_full_name_logging}. Details: {rle_repo}")
        repo_data["processing_error"] = f"GitHub API Rate Limit Error: {rle_repo}"
    except github_gql.TransportQueryError as gql_err: 
        current_logger.error(f"GraphQL query error for {repo_full_name_logging}: {gql_err.errors}")
        if "processing_error" not in repo_data: 
            repo_data["processing_error"] = f"GraphQL Query Error: {gql_err.errors}"
    except GithubException as gh_err_repo: 
        current_logger.error(f"GitHub API (PyGithub context) error processing repo {repo_full_name_logging}: {gh_err_repo.status} {getattr(gh_err_repo, 'data', str(gh_err_repo))}.", exc_info=False)
        repo_data["processing_error"] = f"GitHub API Error: {gh_err_repo.status}"
    except Exception as e_repo:
        current_logger.error(f"CRITICAL UNEXPECTED ERROR in _process_single_github_repository for {repo_full_name_logging}: {e_repo}.", exc_info=True)
        repo_data["processing_error"] = f"Unexpected Error: {e_repo}"

    current_logger.info(f"END _process_single_github_repository (with error: {repo_data.get('processing_error')}) for {repo_full_name_logging}")
    return repo_data

def _get_repo_stubs_and_estimate_api_calls(
    organization_obj: Any, 
    org_name: str, 
    fixed_private_filter_date: datetime,
    hours_per_commit: Optional[float],
    cfg_obj: Optional[Any],
    logger_instance: logging.Logger,
    previous_scan_cache: Dict[str, Dict],
    gql_client_for_estimation: Optional[github_gql.Client] # For fetching live SHAs
) -> tuple[List[Dict[str, Any]], int]:
    """
    Internal helper to list repository stubs, filter them, and estimate API calls.
    Returns a list of enriched repository info dicts and the estimated API calls for them.
    """
    logger_instance.info(f"Analyzing all repository stubs for '{org_name}'...  Be patient, this may take a while...")
    github_cache_config = PLATFORM_CACHE_CONFIG["github"]

    all_repo_stubs_in_org = []
    try:
        repo_paginator = organization_obj.get_repos(type='all')
        all_repo_stubs_in_org = list(repo_paginator)
        logger_instance.info(f"Found {len(all_repo_stubs_in_org)} repositories in '{org_name}'..")
    except RateLimitExceededException as rle_list:
        logger_instance.error(f"GitHub API rate limit hit while listing repositories for '{org_name}': {rle_list}. Cannot proceed with this target.")
        raise 
    except Exception as e_list:
        logger_instance.error(f"Error listing repositories for '{org_name}': {e_list}. Cannot proceed.", exc_info=True)
        raise 

    enriched_repos_list: List[Dict[str, Any]] = []
    api_calls_for_listing = (len(all_repo_stubs_in_org) // 100) + 1
    api_calls_for_sha_checks_gql_in_estimation = 0
    api_calls_for_full_processing_gql_estimation = 0
    skipped_by_date_filter_count = 0
    skipped_empty_repo_count = 0

    # --- Configuration for GQL Retries during pre-scan peek ---
    # Use existing GQL retry configs if available, or sensible defaults
    if cfg_obj and hasattr(cfg_obj, 'GITHUB_GQL_MAX_RETRIES_ENV'):
        max_gql_peek_retries = int(getattr(cfg_obj, 'GITHUB_GQL_MAX_RETRIES_ENV'))
    else:
        max_gql_peek_retries = int(os.getenv("GITHUB_GQL_MAX_RETRIES", "2")) # Slightly less aggressive for peeks

    if cfg_obj and hasattr(cfg_obj, 'GITHUB_GQL_INITIAL_RETRY_DELAY_ENV'):
        initial_gql_peek_delay = float(getattr(cfg_obj, 'GITHUB_GQL_INITIAL_RETRY_DELAY_ENV'))
    else:
        initial_gql_peek_delay = float(os.getenv("GITHUB_GQL_INITIAL_RETRY_DELAY", "30")) # Shorter initial for peeks

    if cfg_obj and hasattr(cfg_obj, 'GITHUB_GQL_RETRY_BACKOFF_FACTOR_ENV'):
        gql_peek_backoff_factor = float(getattr(cfg_obj, 'GITHUB_GQL_RETRY_BACKOFF_FACTOR_ENV'))
    else:
        gql_peek_backoff_factor = float(os.getenv("GITHUB_GQL_RETRY_BACKOFF_FACTOR", "1.5"))

    MAX_INDIVIDUAL_PEEK_RETRY_DELAY_SECONDS = float(os.getenv("GITHUB_GQL_MAX_INDIVIDUAL_RETRY_DELAY", "300")) # Cap peek retries

    # Constants for full scan estimation
    API_CALLS_PER_FULL_GITHUB_GQL_SCAN_ESTIMATE = 1 # Main GQL call
    est_calls_labor_github = getattr(cfg_obj, 'ESTIMATED_LABOR_CALLS_PER_REPO_GITHUB_ENV',
                                   getattr(cfg_obj, 'ESTIMATED_LABOR_CALLS_PER_REPO_ENV', "3")) \
                                   if hours_per_commit else "0"

    for repo_stub in all_repo_stubs_in_org:
        include_repo = False
        if not repo_stub.private:   # if public repo, always include
            include_repo = True
        else: # if private repo, check last modified date...
            created_at_dt = repo_stub.created_at.replace(tzinfo=timezone.utc) if repo_stub.created_at else None
            modified_at_dt = repo_stub.pushed_at.replace(tzinfo=timezone.utc) if repo_stub.pushed_at else None
            if (created_at_dt and created_at_dt >= fixed_private_filter_date) or \
               (modified_at_dt and modified_at_dt >= fixed_private_filter_date):
                include_repo = True
            else:
                skipped_by_date_filter_count += 1

        # NEW: Add check for empty repository using repo_stub.size
        if include_repo: # If it's still a candidate after privacy/date filters
            # Check if the repository is empty using the 'size' attribute from the REST API stub.
            # This attribute should be available from the initial listing of repositories.
            # A size of 0 typically indicates an empty repository.
            if hasattr(repo_stub, 'size') and repo_stub.size == 0:
                logger_instance.info(f"Pre-scan: Repo '{repo_stub.full_name}' identified as empty (size: 0 from REST stub). Will be excluded from scan.")
                include_repo = False 
                skipped_empty_repo_count += 1

        if include_repo:
            repo_id_str = str(repo_stub.id) if hasattr(repo_stub, 'id') and repo_stub.id else None
            repo_name_for_log = repo_stub.full_name
            live_sha: Optional[str] = None
            live_sha_date: Optional[datetime] = None # From GQL 'pushedAt' on default branch

            if gql_client_for_estimation and repo_id_str: # Need ID for reliable caching
                for attempt in range(max_gql_peek_retries + 1):
                    try:
                        if attempt > 0:
                            delay = initial_gql_peek_delay * (gql_peek_backoff_factor ** (attempt - 1))
                            delay = min(delay, MAX_INDIVIDUAL_PEEK_RETRY_DELAY_SECONDS)
                            logger_instance.info(
                                f"Pre-scan GQL Peek Rate Limit Retry {attempt}/{max_gql_peek_retries} for {repo_name_for_log}. "
                                f"Waiting {delay:.2f}s..."
                            )
                            time.sleep(delay)

                        peek_data = github_gql.fetch_repository_short_metadata_graphql(
                            client=gql_client_for_estimation, owner=org_name, repo_name=repo_stub.name, logger_instance=logger_instance
                        )
                        api_calls_for_sha_checks_gql_in_estimation += 1 # Count successful or final failed attempt
                        if peek_data:
                            live_sha = peek_data.get('lastCommitSHA')
                            repo_id_str = str(peek_data.get('id')) if peek_data.get('id') is not None else repo_id_str
                            pushed_at_str_gql = peek_data.get('pushedAt')
                            if pushed_at_str_gql:
                                live_sha_date = datetime.fromisoformat(pushed_at_str_gql.replace('Z', '+00:00')).replace(tzinfo=timezone.utc)
                        break # Success
                    except github_gql.TransportQueryError as gql_err_peek:
                        is_rate_limited_peek = False
                        if gql_err_peek.errors and isinstance(gql_err_peek.errors, list):
                            for err_detail in gql_err_peek.errors:
                                if isinstance(err_detail, dict) and err_detail.get('type') == 'RATE_LIMITED':
                                    is_rate_limited_peek = True
                                    break
                        if is_rate_limited_peek:
                            logger_instance.warning(f"Pre-scan GQL Peek: RATE_LIMITED on attempt {attempt + 1} for {repo_name_for_log}. Details: {gql_err_peek.errors}")
                            if attempt < max_gql_peek_retries:
                                continue
                            else:
                                logger_instance.error(f"Pre-scan GQL Peek: Max retries ({max_gql_peek_retries}) for RATE_LIMITED error reached for {repo_name_for_log}. Proceeding without peek data.")
                                break 
                        else: # Non-rate-limit GQL error
                            logger_instance.warning(f"Pre-scan GQL Peek: TransportQueryError (not rate limit) for {repo_name_for_log}: {gql_err_peek.errors}")
                            break 
                    except Exception as e_peek:
                        logger_instance.warning(f"Pre-scan GQL Peek: Unexpected error for {repo_name_for_log}: {e_peek}")
                        if attempt == 0: api_calls_for_sha_checks_gql_in_estimation += 1 # Count first attempt if it fails unexpectedly
                        break 

            is_cached = repo_id_str in previous_scan_cache if repo_id_str else False
            is_changed = False
            if is_cached and repo_id_str:
                cached_sha = previous_scan_cache[repo_id_str].get(github_cache_config["commit_sha_field"])
                # User's desired logic: is_changed = (cached_sha != live_sha)
                # This handles live_sha being None correctly based on Python's '!=' behavior.
                is_changed = (cached_sha != live_sha)
            else: # Not cached
                # if the repo was changed after the establisged date (June 21, 2025) then is_changed = true and is_desired_for_processing = true
                if live_sha_date:
                    is_changed = live_sha_date >= fixed_private_filter_date
                else:
                    is_changed = True # If live_sha_date is None or not available, consider the repo changed.
            repo_visibility = "private" if repo_stub.private else "public"
            is_desired_for_processing = (repo_visibility == "public") or is_changed or is_cached

            enriched_repo_info = {
                "repo_stub_obj": repo_stub,
                "repo_id_str": repo_id_str,
                "repo_name_for_log": repo_name_for_log,
                "live_sha": live_sha,
                "live_sha_date": live_sha_date,
                "visibility": repo_visibility,
                "is_cached": is_cached,
                "is_changed": is_changed,
                "is_desired_for_processing": is_desired_for_processing,
            }
            enriched_repos_list.append(enriched_repo_info)
            # undergo full scann if not is_cached OR is_changed
            if is_desired_for_processing and not (is_cached and not is_changed):
                api_calls_for_full_processing_gql_estimation += API_CALLS_PER_FULL_GITHUB_GQL_SCAN_ESTIMATE
                if hours_per_commit:
                    api_calls_for_full_processing_gql_estimation += int(est_calls_labor_github)

    total_estimated_calls = api_calls_for_listing + api_calls_for_sha_checks_gql_in_estimation + api_calls_for_full_processing_gql_estimation
    logger_instance.info(f"Identified {len(enriched_repos_list)} repositories to potentially process for '{org_name}'. Estimated API calls for this target: {total_estimated_calls}")
    if skipped_empty_repo_count > 0:
        logger_instance.info(f"Skipped {skipped_empty_repo_count} empty repositories from '{org_name}'.")   
    if skipped_by_date_filter_count > 0:
        logger_instance.info(f"Skipped {skipped_by_date_filter_count} private repositories from '{org_name}' due to fixed date filter ({fixed_private_filter_date.strftime('%Y-%m-%d')}).")
    return enriched_repos_list, total_estimated_calls

def estimate_api_calls_for_org(
    token: Optional[str],
    org_name: str,
    github_instance_url: Optional[str],
    cfg_obj: Optional[Any],
    logger_instance: logging.Logger # Made non-optional
) -> Tuple[List[Dict[str, Any]], int]: # Returns enriched list and estimate
    """
    Estimates the number of API calls required to process a GitHub organization.
    This is used by the orchestrator for pre-scan estimation.
    """
    current_logger = logger_instance # Directly use the passed-in adapter
    current_logger.info(f"Estimating API calls for GitHub organization: {org_name}")

    if is_placeholder_token(token):
        current_logger.error("GitHub token is a placeholder or missing. Cannot estimate API calls.")
        return [], 0

    fixed_private_filter_date = get_fixed_private_filter_date(cfg_obj, current_logger) 
    gh_pygithub_client, organization_obj, gql_client_for_est, _ = _initialize_clients_for_org(token, org_name, github_instance_url, current_logger)
    if not organization_obj or not gql_client_for_est:
        return 0 

    # Load cache for estimation
    previous_intermediate_filepath = os.path.join(cfg_obj.OUTPUT_DIR, f"intermediate_github_{org_name.replace('/', '_')}.json")
    previous_scan_cache_for_estimation = load_previous_scan_data(previous_intermediate_filepath, "github")

    try:
        hours_per_commit_val = None
        if cfg_obj and hasattr(cfg_obj, 'HOURS_PER_COMMIT_ENV'):
            hours_per_commit_val = getattr(cfg_obj, 'HOURS_PER_COMMIT_ENV')
        else:
            hours_per_commit_val = os.getenv('HOURS_PER_COMMIT_ENV')
        
        hpc_float: Optional[float] = None
        if hours_per_commit_val is not None:
            try:
                hpc_float = float(hours_per_commit_val)
            except ValueError:
                current_logger.warning(f"Invalid format for HOURS_PER_COMMIT_ENV: '{hours_per_commit_val}'. Labor hours estimation might be skipped or use default.")

        enriched_list, estimated_calls = _get_repo_stubs_and_estimate_api_calls(
            organization_obj, org_name, fixed_private_filter_date,
            hpc_float, cfg_obj, current_logger, previous_scan_cache_for_estimation, gql_client_for_est
        )
        return enriched_list, estimated_calls
    except Exception: 
        return [], 0

def fetch_repositories(
    token: Optional[str],
    org_name: str,
    processed_counter: List[int],
    processed_counter_lock: threading.Lock,
    logger_instance: logging.LoggerAdapter, # Made non-optional  
    debug_limit: int | None = None,
    github_instance_url: str | None = None,
    hours_per_commit: Optional[float] = None,
    max_workers: int = 5, # Default value
    cfg_obj: Optional[Any] = None,
    previous_scan_output_file: Optional[str] = None,
    # New parameters for pre-fetched data and global delay
    pre_fetched_enriched_repos: Optional[List[Dict[str, Any]]] = None,
    global_inter_submission_delay: Optional[float] = None,
    # Pass GQL client and endpoint for workers if already initialized
    gql_client_for_workers: Optional[github_gql.Client] = None, # Can be the same as gql_client_for_peek
    graphql_endpoint_url_for_workers: Optional[str] = None
) -> list[dict]:
    """
    Fetches repository details from a specific GitHub organization.
    Uses PyGithub for listing repositories (if not pre-fetched) and a GraphQL client for fetching details.
    """
    instance_msg = f"GitHub instance: {github_instance_url}" if github_instance_url else "public GitHub.com"
    
    current_logger = logger_instance # Directly passed-in adapter
    current_logger.info(f"Attempting to fetch repositories for GitHub organization: {ANSI_YELLOW}{org_name} on {instance_msg}{ANSI_RESET} (max_workers: {max_workers})")

    if is_placeholder_token(token):
        current_logger.error("GitHub token is a placeholder or missing. Cannot fetch repositories.")
        return []

    fixed_private_filter_date = get_fixed_private_filter_date(cfg_obj, current_logger)

    previous_scan_cache: Dict[str, Dict] = {}
    if previous_scan_output_file:
        current_logger.info(f"Attempting to load previous GitHub scan data for '{org_name}' from: {previous_scan_output_file}")
        loaded_cache = load_previous_scan_data(previous_scan_output_file, "github")
        if isinstance(loaded_cache, dict): previous_scan_cache = loaded_cache
        else: current_logger.warning(f"CACHE: load_previous_scan_data did not return a dict for {previous_scan_output_file}. Cache will be empty.")
    else:
        current_logger.info(f"No previous scan output file provided for GitHub org '{org_name}'. Full scan for all repos in this org.")
    
    if pre_fetched_enriched_repos is not None:
        enriched_repo_list = pre_fetched_enriched_repos
        current_logger.info(f"Using pre-fetched enriched repository list for '{org_name}'.")
        # If enriched_repo_list is pre-fetched, assume gql_client_for_workers and graphql_endpoint_url_for_workers are also passed if needed
        # or initialize them here if not.
        if not gql_client_for_workers or not graphql_endpoint_url_for_workers:
            _, _, gql_client_for_workers, graphql_endpoint_url_for_workers = _initialize_clients_for_org(
                token, org_name, github_instance_url, current_logger # PyGithub client and org_obj not strictly needed here if stubs are pre-fetched
            )
    else:
        # This path is less optimal if generate_codejson.py is doing the pre-fetching
        current_logger.warning(f"No pre-fetched enriched repository list for '{org_name}'. Fetching now (less optimal).")
        gh_pygithub_client, organization_obj, gql_client_for_est, _ = _initialize_clients_for_org(
            token, org_name, github_instance_url, current_logger
        )
        if not organization_obj:
            return []
        # Initialize GQL client for workers if not already done
        if not gql_client_for_workers or not graphql_endpoint_url_for_workers:
             _, _, gql_client_for_workers, graphql_endpoint_url_for_workers = _initialize_clients_for_org(
                token, org_name, github_instance_url, current_logger
            )
        try:
            enriched_repo_list, _ = _get_repo_stubs_and_estimate_api_calls(
                organization_obj, org_name, fixed_private_filter_date, hours_per_commit, 
                cfg_obj, current_logger, previous_scan_cache, gql_client_for_est
            )
        except Exception:
            current_logger.error(f"Failed to get repository stubs or estimate calls for '{org_name}'. Aborting target.", exc_info=True)
            return []

    if not enriched_repo_list:
        current_logger.info(f"No repositories to process for '{org_name}' after filtering (or from pre-fetched data). Skipping further processing for this target.")
        return []

    # Determine inter_submission_delay
    if global_inter_submission_delay is not None:
        inter_submission_delay = global_inter_submission_delay
        current_logger.info(f"Using globally calculated inter-submission delay: {inter_submission_delay:.3f}s for GitHub org '{org_name}'.")
    else:
        # Fallback if global delay not provided (less optimal)
        current_logger.warning(f"Global inter-submission delay not provided for GitHub org '{org_name}'. Calculating locally (less optimal).")
        # Need a PyGithub client for rate limit status if not already available
        temp_gh_client_for_rate_limit, temp_org_obj_for_rate_limit, _, _ = _initialize_clients_for_org(token, org_name, github_instance_url, current_logger)
        if not temp_gh_client_for_rate_limit or not temp_org_obj_for_rate_limit: return []
        
        current_rate_limit_status = get_github_rate_limit_status(temp_gh_client_for_rate_limit, current_logger)
        if not current_rate_limit_status:
            current_logger.error(f"Could not determine current rate limit for '{org_name}'. Aborting target.")
            return []
        
        # Re-estimate calls for this target if not pre-fetched (suboptimal path)
        _, estimated_api_calls_for_current_target_fallback = _get_repo_stubs_and_estimate_api_calls(
            temp_org_obj_for_rate_limit, org_name, fixed_private_filter_date, hours_per_commit,
            cfg_obj, current_logger, previous_scan_cache, gql_client_for_workers # Use worker GQL client for estimation
        )
        inter_submission_delay = calculate_inter_submission_delay(
            rate_limit_status=current_rate_limit_status,
            estimated_api_calls_for_target=estimated_api_calls_for_current_target_fallback,
            num_workers=max_workers,
            safety_factor=float(os.getenv('API_SAFETY_FACTOR_ENV', "0.8")), # Simplified access for fallback
            min_delay_seconds=float(os.getenv('MIN_INTER_REPO_DELAY_SECONDS_ENV', "0.1")),
            max_delay_seconds=float(os.getenv('MAX_INTER_REPO_DELAY_SECONDS_ENV', "30.0"))
        )
    
    safety_factor_val = os.getenv('API_SAFETY_FACTOR_ENV', "0.8")
    if cfg_obj and hasattr(cfg_obj, 'API_SAFETY_FACTOR_ENV'):
        safety_factor_val = getattr(cfg_obj, 'API_SAFETY_FACTOR_ENV', safety_factor_val)
    
    min_delay_val = os.getenv('MIN_INTER_REPO_DELAY_SECONDS_ENV', "0.1")
    if cfg_obj and hasattr(cfg_obj, 'MIN_INTER_REPO_DELAY_SECONDS_ENV'):
        min_delay_val = getattr(cfg_obj, 'MIN_INTER_REPO_DELAY_SECONDS_ENV', min_delay_val)

    max_delay_val = os.getenv('MAX_INTER_REPO_DELAY_SECONDS_ENV', "30.0")
    if cfg_obj and hasattr(cfg_obj, 'MAX_INTER_REPO_DELAY_SECONDS_ENV'):
        max_delay_val = getattr(cfg_obj, 'MAX_INTER_REPO_DELAY_SECONDS_ENV', max_delay_val)

    github_cache_config = PLATFORM_CACHE_CONFIG["github"]
    processed_repo_list: List[Dict[str, Any]] = []
    repo_count_for_org_processed_or_submitted = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_repo_name = {}
        try:
            for enriched_repo in enriched_repo_list:
                with processed_counter_lock:
                    if debug_limit is not None and processed_counter[0] >= debug_limit:
                        current_logger.info(f"Global debug limit ({debug_limit}) reached. Stopping submissions for {org_name}.")
                        break
                    processed_counter[0] += 1
                
                repo_stub_obj = enriched_repo["repo_stub_obj"]
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
                    # Potential "peek for NEXT repo" logic could go here if actual_delay_this_submission is high

                current_logger.info(f"Submission delay for {repo_name_for_log}: {log_message_suffix}", extra={'org_group': org_name})
                if actual_delay_this_submission > 0:
                    time.sleep(actual_delay_this_submission)


                repo_count_for_org_processed_or_submitted +=1
                future = executor.submit(
                    _process_single_github_repository,
                    repo_stub_obj, org_name=org_name, token=token, # Pass original stub
                    github_instance_url=github_instance_url,
                    hours_per_commit=hours_per_commit, cfg_obj=cfg_obj,
                    graphql_endpoint_url_for_client=graphql_endpoint_url_for_workers, # Use worker GQL endpoint
                    previous_scan_cache=previous_scan_cache,
                    num_repos_in_target=len(enriched_repo_list),
                    num_workers=max_workers,
                    logger_instance=current_logger,
                    live_commit_sha_from_prescan=enriched_repo['live_sha'], # Pass live SHA from pre-scan
                    live_repo_id_from_prescan=enriched_repo['repo_id_str']  # Pass live ID from pre-scan
                )
                future_to_repo_name[future] = repo_name_for_log
        
        except RateLimitExceededException as rle_iter: current_logger.error(f"GitHub API rate limit (PyGithub listing context) for {org_name}. Details: {rle_iter}")
        except GithubException as gh_ex_iter: current_logger.error(f"GitHub API error (PyGithub listing context) for {org_name}: {gh_ex_iter}.")
        except Exception as ex_iter: current_logger.error(f"Unexpected error (PyGithub listing context) for {org_name}: {ex_iter}.")

        for future in as_completed(future_to_repo_name):
            repo_name_for_log = future_to_repo_name[future]
            try:
                repo_data_result = future.result()
                if repo_data_result and repo_data_result.get("processing_status") != "skipped_fork":
                    processed_repo_list.append(repo_data_result)
            except Exception as exc:
                current_logger.error(f"Repo {repo_name_for_log} generated an exception: {exc}", exc_info=True)
                processed_repo_list.append({"name": repo_name_for_log.split('/')[-1], "organization": org_name, "processing_error": f"Thread execution failed: {exc}"})

    current_logger.info(f"Finished processing for {repo_count_for_org_processed_or_submitted} repos from GitHub org: {org_name}. Collected {len(processed_repo_list)} results.")
    return processed_repo_list

def _initialize_clients_for_org(
    token: Optional[str],
    org_name: str,
    github_instance_url: Optional[str],
    logger_instance: logging.LoggerAdapter 
) -> tuple[Optional[Github], Optional[Any], Optional[github_gql.Client], Optional[str]]:
    """Initializes PyGithub client, gets organization object, and determines GraphQL endpoint."""
    try:
        pygithub_base_url = None
        if github_instance_url:
            pygithub_base_url = github_instance_url.rstrip('/') + "/api/v3" if not github_instance_url.endswith("/api/v3") else github_instance_url
        
        ssl_verify_flag = os.getenv("DISABLE_SSL_VERIFICATION", "false").lower() != "true"
        if not ssl_verify_flag:
           logger_instance.warning(f"{ANSI_RED}SECURITY WARNING: SSL certificate verification is DISABLED for GitHub connections.{ANSI_RESET}")

        effective_pygithub_url = pygithub_base_url if pygithub_base_url else "https://api.github.com"
        gh_pygithub_client = Github(login_or_token=token, base_url=effective_pygithub_url, verify=ssl_verify_flag, timeout=30)
        
        organization_obj = gh_pygithub_client.get_organization(org_name)
        logger_instance.info(f"Successfully configured PyGithub client for organization: {org_name}.")
        
        # Determine GraphQL endpoint URL for GQL client
        graphql_api_url_for_gql_client = None
        if github_instance_url: 
            # For GQL client, it needs the base URL if GHES, not the /api/graphql part yet
            # The get_github_gql_client will append /api/graphql if needed
            temp_base = github_instance_url.rstrip('/')
            if temp_base.endswith("/api/v3"): # Correctly strip /api/v3 if present
                graphql_api_url_for_gql_client = temp_base[:-len("/api/v3")]
            else: # Assume it's already a base URL or needs /api/graphql appended by client
                graphql_api_url_for_gql_client = temp_base
        gql_client = github_gql.get_github_gql_client(token, graphql_api_url_for_gql_client)
        logger_instance.info(f"GraphQL client initialized. Endpoint for threads will be based on: {graphql_api_url_for_gql_client or github_gql.GITHUB_GRAPHQL_ENDPOINT}).")
        return gh_pygithub_client, organization_obj, gql_client, graphql_api_url_for_gql_client 
    except Exception as e:
        logger_instance.critical(f"Failed to initialize GitHub clients for org '{org_name}': {e}", exc_info=True)
        return None, None, None, None
