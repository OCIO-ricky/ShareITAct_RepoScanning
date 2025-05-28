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
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Optional, Any
from datetime import timezone, datetime

from utils.delay_calculator import calculate_dynamic_delay
from utils.caching import load_previous_scan_data, PLATFORM_CACHE_CONFIG
from utils.dateparse import parse_repos_created_after_date
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

logger = logging.getLogger(__name__)
PLACEHOLDER_GITHUB_TOKEN = "YOUR_GITHUB_PAT"


def apply_dynamic_github_delay(cfg_obj: Optional[Any], num_repos_in_target: Optional[int], num_workers: int = 1, is_graphql_call: bool = False):
    """
    Calculates and applies a dynamic delay.
    Adjusted for potentially different delay settings for GraphQL vs REST.
    """
    delay_seconds = 0.0
    if cfg_obj:
        if is_graphql_call:
            base_delay = float(getattr(cfg_obj, 'GITHUB_GRAPHQL_CALL_DELAY_SECONDS_ENV', os.getenv("GITHUB_GRAPHQL_CALL_DELAY_SECONDS", "0.25")))
            max_d = float(getattr(cfg_obj, 'GITHUB_GRAPHQL_MAX_DELAY_SECONDS_ENV', os.getenv("GITHUB_GRAPHQL_MAX_DELAY_SECONDS", "0.75")))
        else: # REST call (e.g., listing repos, getting initial branch SHA if not via GQL first)
            base_delay = float(getattr(cfg_obj, 'GITHUB_POST_API_CALL_DELAY_SECONDS_ENV', os.getenv("GITHUB_POST_API_CALL_DELAY_SECONDS", "0.1")))
            max_d = float(getattr(cfg_obj, 'DYNAMIC_DELAY_MAX_SECONDS_ENV', os.getenv("DYNAMIC_DELAY_MAX_SECONDS", "1.0")))

        threshold = int(getattr(cfg_obj, 'DYNAMIC_DELAY_THRESHOLD_REPOS_ENV', os.getenv("DYNAMIC_DELAY_THRESHOLD_REPOS", "100")))
        scale = float(getattr(cfg_obj, 'DYNAMIC_DELAY_SCALE_FACTOR_ENV', os.getenv("DYNAMIC_DELAY_SCALE_FACTOR", "1.5")))

        delay_seconds = calculate_dynamic_delay(
            base_delay_seconds=base_delay,
            num_items=num_repos_in_target if num_repos_in_target is not None and num_repos_in_target > 0 else None,
            threshold_items=threshold,
            scale_factor=scale,
            max_delay_seconds=max_d,
            num_workers=num_workers
        )

    if delay_seconds > 0:
        call_type = "GraphQL" if is_graphql_call else "REST API"
        logger.debug(f"Applying SYNC dynamic GitHub {call_type} call delay: {delay_seconds:.2f}s "
                     f"(based on target size: {num_repos_in_target}, workers: {num_workers})")
        time.sleep(delay_seconds)

def is_placeholder_token(token: Optional[str]) -> bool:
    """Checks if the GitHub token is missing or a known placeholder."""
    return not token or token == PLACEHOLDER_GITHUB_TOKEN

def _process_single_github_repository(
    repo_stub, # Can be a PyGithub Repository stub or full object
    org_name: str,
    token: Optional[str],
    github_instance_url: Optional[str],
    hours_per_commit: Optional[float],
    cfg_obj: Any,
    inter_repo_adaptive_delay_seconds: float,
    num_repos_in_target: Optional[int],
    # gql_client: Any, # No longer pass shared client
    graphql_endpoint_url_for_client: Optional[str], # Pass URL to create client
    previous_scan_cache: Dict[str, Dict],
    # current_commit_sha is now fetched via GraphQL or from repo_stub if available
    num_workers: int = 1
) -> Dict[str, Any]:
    """
    Processes a single GitHub repository using GraphQL to extract its metadata.
    """
    # Use repo_stub.name and org_name for initial identification
    repo_name_for_gql = repo_stub.name
    repo_full_name_logging = f"{org_name}/{repo_name_for_gql}"
    repo_data: Dict[str, Any] = {"name": repo_name_for_gql, "organization": org_name}
    github_cache_config = PLATFORM_CACHE_CONFIG["github"]
    
    # Try to get repo_id from stub if available, otherwise GQL will provide databaseId
    # The key for caching should be consistent. If repo_stub.id is reliable, use it.
    # Otherwise, we might need to adjust caching strategy or ensure GQL provides the same ID.
    # For now, assume repo_stub.id is the numeric REST API ID used for caching.
    repo_id_str = str(repo_stub.id) if hasattr(repo_stub, 'id') and repo_stub.id else None

    # Ensure org_group_context uses only the organization name for logging
    # consistency, rather than org_name/repo_name.
    org_group_context = org_name

    # --- Caching Logic ---
    # Fetch current_commit_sha via GraphQL as part of the main data fetch.
    # If repo_id_str is not available yet, we can't do a cache lookup before GQL.
    # This means the GQL call will happen, and then we check its SHA against cache.
    # This is slightly less efficient than SHA-first caching but simplifies GQL integration.
    # A more advanced approach could be a small GQL query for just SHA, then the main one.

    logger.info(f"Processing GitHub repository: {repo_full_name_logging} (ID: {repo_id_str or 'Unknown initially'}) with GraphQL.", extra={'org_group': org_group_context})

    try:
        # Create a new GraphQL client instance for this specific thread/task
        # This ensures no conflict with shared client state.
        client_for_this_task = github_gql.get_github_gql_client(token, graphql_endpoint_url_for_client)
        if not client_for_this_task: # Should not happen if get_github_gql_client is robust
            logger.error(f"Failed to create GraphQL client for {repo_full_name_logging}. Skipping.", extra={'org_group': org_group_context})
            repo_data["processing_error"] = "GraphQL client creation failed"
            return repo_data

        # Apply delay before the main GraphQL call
        apply_dynamic_github_delay(cfg_obj, num_repos_in_target, num_workers, is_graphql_call=True)

        gql_data = github_gql.fetch_repository_details_graphql(client_for_this_task, org_name, repo_name_for_gql)

        if not gql_data:
            logger.error(f"Failed to fetch GraphQL data for {repo_full_name_logging}. Skipping.", extra={'org_group': org_group_context})
            repo_data["processing_error"] = "GraphQL data fetch failed"
            return repo_data

        # Update repo_id_str if it wasn't available from stub or if GQL provides a more canonical one
        if not repo_id_str and gql_data.get("databaseId"):
            repo_id_str = str(gql_data["databaseId"])
        elif gql_data.get("databaseId") and str(gql_data["databaseId"]) != repo_id_str:
            # This case needs careful consideration if stub ID and GQL databaseId differ
            # For now, prefer GQL's databaseId if available and different.
            logger.warning(f"Repo ID mismatch for {repo_full_name_logging}: stub ID {repo_id_str}, GQL databaseId {gql_data['databaseId']}. Using GQL ID for processing.", extra={'org_group': org_group_context})
            repo_id_str = str(gql_data["databaseId"])
        
        repo_data["repo_id"] = int(repo_id_str) if repo_id_str else None


        # Now that we have gql_data (including commit SHA and repo_id), check cache
        gql_current_commit_sha = None
        if gql_data.get("defaultBranchRef") and gql_data["defaultBranchRef"].get("target"):
            gql_current_commit_sha = gql_data["defaultBranchRef"]["target"].get("oid")

        if gql_current_commit_sha and repo_id_str:
            cached_repo_entry = previous_scan_cache.get(repo_id_str)
            if cached_repo_entry:
                cached_commit_sha = cached_repo_entry.get(github_cache_config["commit_sha_field"])
                if cached_commit_sha and gql_current_commit_sha == cached_commit_sha:
                    logger.info(f"CACHE HIT (post-GraphQL): GitHub repo '{gql_data.get('nameWithOwner', repo_full_name_logging)}' (ID: {repo_id_str}) has not changed. Using cached data.", extra={'org_group': org_group_context})
                    repo_data_to_process = cached_repo_entry.copy()
                    repo_data_to_process[github_cache_config["commit_sha_field"]] = gql_current_commit_sha
                    if cfg_obj:
                        repo_data_to_process = exemption_processor.process_repository_exemptions(
                            repo_data_to_process,
                            org_name,
                            default_org_identifiers=[org_name],
                            ai_is_enabled_from_config=cfg_obj.AI_ENABLED_ENV, ai_model_name_from_config=cfg_obj.AI_MODEL_NAME_ENV,
                            ai_temperature_from_config=cfg_obj.AI_TEMPERATURE_ENV, ai_max_output_tokens_from_config=cfg_obj.AI_MAX_OUTPUT_TOKENS_ENV,
                            ai_max_input_tokens_from_config=cfg_obj.MAX_TOKENS_ENV)
                    return repo_data_to_process
        logger.info(f"CACHE MISS or no prior SHA (post-GraphQL): Processing full data for {gql_data.get('nameWithOwner', repo_full_name_logging)}.", extra={'org_group': org_group_context})


        repo_full_name_logging = gql_data.get("nameWithOwner", repo_full_name_logging) # Update with more accurate name

        if gql_data.get("isFork"):
            logger.info(f"Skipping forked repository: {repo_full_name_logging}", extra={'org_group': org_group_context})
            repo_data["processing_status"] = "skipped_fork"
            return repo_data

        repo_data['_is_empty_repo'] = False
        # GraphQL diskUsage is in kilobytes. 0 can mean empty or very small.
        # Check for defaultBranchRef as well, as truly empty repos might not have one.
        if gql_data.get("diskUsage", 0) == 0 and not gql_data.get("defaultBranchRef"):
            logger.info(f"Repository {repo_full_name_logging} appears empty based on GraphQL (diskUsage: 0, no defaultBranchRef).", extra={'org_group': org_group_context})
            repo_data['_is_empty_repo'] = True
            # Populate minimal fields for empty repo
            repo_data.update({
                "description": gql_data.get("description") or "",
                "repositoryURL": gql_data.get("url"), # type: ignore
                "vcs": "git", # type: ignore
                "repositoryVisibility": gql_data.get("visibility", "private").lower(),
                "status": "development", "version": "N/A", "laborHours": 0,
                "languages": [], "tags": [],
                "date": {
                    "created": datetime.fromisoformat(gql_data["createdAt"].replace('Z', '+00:00')).isoformat() if gql_data.get("createdAt") else None,
                    "lastModified": datetime.fromisoformat(gql_data["pushedAt"].replace('Z', '+00:00')).isoformat() if gql_data.get("pushedAt") else None,
                },
                "permissions": {"usageType": "openSource", "exemptionText": None, "licenses": []}, # type: ignore
                "contact": {}, # type: ignore
                "archived": gql_data.get("isArchived", False) # type: ignore
            })
            if gql_current_commit_sha:
                 repo_data[github_cache_config["commit_sha_field"]] = gql_current_commit_sha
            if cfg_obj:
                repo_data = exemption_processor.process_repository_exemptions(
                    repo_data,
                    org_name,
                    default_org_identifiers=[org_name],
                    ai_is_enabled_from_config=cfg_obj.AI_ENABLED_ENV, ai_model_name_from_config=cfg_obj.AI_MODEL_NAME_ENV,
                    ai_temperature_from_config=cfg_obj.AI_TEMPERATURE_ENV, ai_max_output_tokens_from_config=cfg_obj.AI_MAX_OUTPUT_TOKENS_ENV,
                    ai_max_input_tokens_from_config=cfg_obj.MAX_TOKENS_ENV)
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

        for i, path in enumerate(github_gql.COMMON_README_PATHS):
            alias_name = path.replace('.', '_').replace('/', '_')
            file_data = gql_data.get(f"file_{alias_name}_{i}")
            if file_data and file_data.get("text") is not None:
                readme_content_str = file_data["text"]
                if default_branch_name and gql_data.get("url"):
                    readme_html_url = f"{gql_data['url']}/blob/{default_branch_name}/{path.lstrip('/')}"
                logger.debug(f"Found README '{path}' for {repo_full_name_logging} via GraphQL.", extra={'org_group': org_group_context})
                break
        if not readme_content_str: logger.debug(f"No common README file found for {repo_full_name_logging} via GraphQL.", extra={'org_group': org_group_context})

        codeowners_content_str: Optional[str] = None
        for i, path in enumerate(github_gql.COMMON_CODEOWNERS_PATHS):
            alias_name = path.replace('.', '_').replace('/', '_')
            file_data = gql_data.get(f"file_{alias_name}_{i}")
            if file_data and file_data.get("text") is not None:
                codeowners_content_str = file_data["text"]
                logger.debug(f"Found CODEOWNERS '{path}' for {repo_full_name_logging} via GraphQL.", extra={'org_group': org_group_context})
                break
        if not codeowners_content_str: logger.debug(f"No CODEOWNERS file found for {repo_full_name_logging} via GraphQL.", extra={'org_group': org_group_context})

        repo_topics = [node["topic"]["name"] for node in gql_data.get("repositoryTopics", {}).get("nodes", []) if node and node.get("topic")]
        repo_git_tags = [node["name"].replace("refs/tags/", "") for node in gql_data.get("tags", {}).get("nodes", []) if node and node.get("name")]

        repo_data.update({
            "description": gql_data.get("description") or "",
            "repositoryURL": gql_data.get("url"), # type: ignore
            "homepageURL": gql_data.get("homepageUrl") or "", # type: ignore
            "downloadURL": None,
            "readme_url": readme_html_url,
            "vcs": "git",
            "repositoryVisibility": repo_visibility,
            "status": "development", "version": "N/A", "laborHours": 0,
            "languages": all_languages_list,
            "tags": repo_topics, # These are GitHub topics
            "date": {
                "created": created_at_dt.isoformat() if created_at_dt else None,
                "lastModified": pushed_at_dt.isoformat() if pushed_at_dt else None,
            },
            "permissions": {"usageType": None, "exemptionText": None, "licenses": licenses_list}, # type: ignore
            "contact": {}, "contractNumber": None, # type: ignore
            "readme_content": readme_content_str,
            "_codeowners_content": codeowners_content_str,
            # "repo_id" was set earlier
            "_api_tags": repo_git_tags, # These are actual Git tags
            "archived": gql_data.get("isArchived", False),
        })
        repo_data.setdefault('_is_empty_repo', False)
        if gql_current_commit_sha:
            repo_data[github_cache_config["commit_sha_field"]] = gql_current_commit_sha

        if hours_per_commit is not None:
            logger.debug(f"Estimating labor hours for GitHub repo: {repo_full_name_logging}", extra={'org_group': org_group_context})
            # analyze_github_repo_sync might need its own GQL query for commit history
            # or adapt to use commit data if fetched by the main GQL query.
            # For simplicity, assume it handles its own data fetching for now.
            actual_default_branch_name_for_commits = None
            if gql_data.get("defaultBranchRef") and gql_data["defaultBranchRef"].get("name"):
                actual_default_branch_name_for_commits = gql_data["defaultBranchRef"]["name"]

            try:
                labor_df = analyze_github_repo_sync(
                    owner=org_name, repo=repo_name_for_gql, token=token,
                    hours_per_commit=hours_per_commit,
                    github_api_url=github_instance_url or "https://api.github.com", 
                    # session=None, # Removed: analyze_github_repo_sync no longer takes session
                    default_branch_override=actual_default_branch_name_for_commits, # Pass the fetched default branch
                    cfg_obj=cfg_obj,
                    num_repos_in_target=num_repos_in_target,
                    is_empty_repo=repo_data.get('_is_empty_repo', False),
                    number_of_workers=num_workers
                )
                if not labor_df.empty:
                    repo_data["laborHours"] = round(float(labor_df["EstimatedHours"].sum()), 2)
                    logger.info(f"Estimated labor hours for {repo_full_name_logging}: {repo_data['laborHours']}", extra={'org_group': org_group_context})
                else:
                    repo_data["laborHours"] = 0.0
            except Exception as e_lh:
                logger.warning(f"Could not estimate labor hours for {repo_full_name_logging}: {e_lh}", exc_info=True, extra={'org_group': org_group_context})
                repo_data["laborHours"] = 0.0

        if cfg_obj:
            repo_data = exemption_processor.process_repository_exemptions(
                repo_data,
                org_name,
                default_org_identifiers=[org_name],
                ai_is_enabled_from_config=cfg_obj.AI_ENABLED_ENV, ai_model_name_from_config=cfg_obj.AI_MODEL_NAME_ENV,
                ai_temperature_from_config=cfg_obj.AI_TEMPERATURE_ENV, ai_max_output_tokens_from_config=cfg_obj.AI_MAX_OUTPUT_TOKENS_ENV,
                ai_max_input_tokens_from_config=cfg_obj.MAX_TOKENS_ENV)
        else: # Should ideally not happen if generate_codejson passes it
            logger.warning(f"cfg_obj not provided to _process_single_github_repository for {repo_full_name_logging}.", extra={'org_group': org_group_context})
            repo_data = exemption_processor.process_repository_exemptions(
                repo_data,
                org_name,
                default_org_identifiers=[org_name]
            )

        if inter_repo_adaptive_delay_seconds > 0:
            logger.debug(f"GitHub repo {repo_full_name_logging}: Applying INTER-REPO adaptive delay of {inter_repo_adaptive_delay_seconds:.2f}s")
            time.sleep(inter_repo_adaptive_delay_seconds)

        return repo_data

    except RateLimitExceededException as rle_repo: # PyGithub specific, less likely with GQL client
        logger.error(f"GitHub API (PyGithub context) rate limit exceeded processing repo {repo_full_name_logging}. Details: {rle_repo}", extra={'org_group': org_group_context})
        repo_data["processing_error"] = f"GitHub API Rate Limit Error: {rle_repo}"
    except github_gql.TransportQueryError as gql_err: # GQL client specific error
        logger.error(f"GraphQL query error for {repo_full_name_logging}: {gql_err.errors}", extra={'org_group': org_group_context})
        repo_data["processing_error"] = f"GraphQL Query Error: {gql_err.errors}"
        # Check if it's a rate limit error from GraphQL
        if gql_err.errors and isinstance(gql_err.errors, list):
            for error_detail in gql_err.errors:
                if isinstance(error_detail, dict) and error_detail.get('type') == 'RATE_LIMITED':
                    logger.warning(f"GraphQL RATE LIMIT detected for {repo_full_name_logging}.", extra={'org_group': org_group_context})
                    # Potentially trigger a longer backoff or specific handling here
                    break
    except GithubException as gh_err_repo: # PyGithub specific, less likely for main data
        logger.error(f"GitHub API (PyGithub context) error processing repo {repo_full_name_logging}: {gh_err_repo.status} {getattr(gh_err_repo, 'data', str(gh_err_repo))}.", exc_info=False, extra={'org_group': org_group_context})
        repo_data["processing_error"] = f"GitHub API Error: {gh_err_repo.status}"
    except Exception as e_repo:
        logger.error(f"Unexpected error processing repo {repo_full_name_logging}: {e_repo}.", exc_info=True, extra={'org_group': org_group_context})
        repo_data["processing_error"] = f"Unexpected Error: {e_repo}"
    
    return repo_data


def fetch_repositories(
    token: Optional[str],
    org_name: str,
    processed_counter: List[int],
    processed_counter_lock: threading.Lock,
    debug_limit: int | None = None,
    github_instance_url: str | None = None,
    hours_per_commit: Optional[float] = None,
    max_workers: int = 5,
    cfg_obj: Optional[Any] = None,
    previous_scan_output_file: Optional[str] = None
) -> list[dict]:
    """
    Fetches repository details from a specific GitHub organization.
    Uses PyGithub for listing repositories and a GraphQL client for fetching details.
    """
    instance_msg = f"GitHub instance: {github_instance_url}" if github_instance_url else "public GitHub.com"
    # Use org_name as the context for this initial log message
    logger.info(f"Attempting to fetch repositories CONCURRENTLY for GitHub organization: {org_name} on {instance_msg} (max_workers: {max_workers})", extra={'org_group': org_name})

    if is_placeholder_token(token):
        logger.error("GitHub token is a placeholder or missing. Cannot fetch repositories.", extra={'org_group': org_name})
        return []

    repos_created_after_filter_date: Optional[datetime] = None
    if cfg_obj and hasattr(cfg_obj, 'REPOS_CREATED_AFTER_DATE'):
        repos_created_after_filter_date = parse_repos_created_after_date(cfg_obj.REPOS_CREATED_AFTER_DATE, logger) # This logger is module level

    previous_scan_cache: Dict[str, Dict] = {}
    if previous_scan_output_file:
        logger.info(f"Attempting to load previous GitHub scan data for '{org_name}' from: {previous_scan_output_file}", extra={'org_group': org_name})
        loaded_cache = load_previous_scan_data(previous_scan_output_file, "github")
        if isinstance(loaded_cache, dict): previous_scan_cache = loaded_cache
        else: logger.warning(f"CACHE: load_previous_scan_data did not return a dict for {previous_scan_output_file}. Cache will be empty.", extra={'org_group': org_name})
    else:
        logger.info(f"No previous scan output file provided for GitHub org '{org_name}'. Full scan for all repos in this org.", extra={'org_group': org_name})

    gh_pygithub_client = None
    organization_obj_for_iteration = None
    graphql_endpoint_url_for_threads = None # To pass to worker threads

    try:
        # --- PyGithub Client for listing ---
        pygithub_base_url = None
        if github_instance_url:
            pygithub_base_url = github_instance_url.rstrip('/') + "/api/v3" if not github_instance_url.endswith("/api/v3") else github_instance_url
        
        ssl_verify_flag = os.getenv("DISABLE_SSL_VERIFICATION", "false").lower() != "true"
        if not ssl_verify_flag:
            logger.warning(f"{ANSI_RED}SECURITY WARNING: SSL certificate verification is DISABLED for GitHub connections.{ANSI_RESET}", extra={'org_group': org_name})

        effective_pygithub_url = pygithub_base_url if pygithub_base_url else "https://api.github.com"
        gh_pygithub_client = Github(login_or_token=token, base_url=effective_pygithub_url, verify=ssl_verify_flag, timeout=30)
        
        apply_dynamic_github_delay(cfg_obj, None, max_workers, is_graphql_call=False) # Delay for REST call
        organization_obj_for_iteration = gh_pygithub_client.get_organization(org_name)
        logger.info(f"Successfully configured PyGithub client for organization: {org_name}.", extra={'org_group': org_name})

        # --- GraphQL Client for details ---
        # Determine GraphQL endpoint: if github_instance_url is GHES, adjust endpoint.
        if github_instance_url: # GHES
            # GQL endpoint for GHES is typically <ghes_url>/api/graphql
            graphql_endpoint_url_for_threads = github_instance_url.rstrip('/') + "/api/graphql"
        # If public GitHub, github_gql.get_github_gql_client will use its default.
        # We don't create a shared client here anymore.
        logger.info(f"GraphQL endpoint for worker threads will be: {graphql_endpoint_url_for_threads or github_gql.GITHUB_GRAPHQL_ENDPOINT}).", extra={'org_group': org_name})

    except Exception as e:
        logger.critical(f"Failed to initialize GitHub clients for org '{org_name}': {e}", exc_info=True, extra={'org_group': org_name})
        return []

    num_repos_in_target = 0
    inter_repo_adaptive_delay_per_repo = 0.0
    live_repo_list_materialized = None

    # --- Determine num_repos_in_target for adaptive delay ---
    # (This logic remains largely the same as it uses PyGithub for counts)
    cached_repo_count_for_target = 0
    if previous_scan_cache:
        github_id_field = PLATFORM_CACHE_CONFIG.get("github", {}).get("id_field", "repo_id")
        valid_cached_repos = [r_data for r_id, r_data in previous_scan_cache.items() if isinstance(r_data, dict) and r_data.get(github_id_field) is not None]
        cached_repo_count_for_target = len(valid_cached_repos)
        if cached_repo_count_for_target > 0:
            num_repos_in_target = cached_repo_count_for_target
            if repos_created_after_filter_date and cfg_obj and hasattr(cfg_obj, 'ADAPTIVE_DELAY_CACHE_MODIFIED_FACTOR_ENV'):
                try:
                    apply_dynamic_github_delay(cfg_obj, None, max_workers, is_graphql_call=False)
                    total_live_repos_for_adjustment = organization_obj_for_iteration.get_repos(type='all').totalCount
                    if total_live_repos_for_adjustment > cached_repo_count_for_target:
                        diff_count = total_live_repos_for_adjustment - cached_repo_count_for_target
                        additional_repos_estimate = int(diff_count * float(cfg_obj.ADAPTIVE_DELAY_CACHE_MODIFIED_FACTOR_ENV))
                        if additional_repos_estimate > 0: num_repos_in_target += additional_repos_estimate
                except Exception as e_adj_count: logger.warning(f"GitHub: Error fetching total live repo count for cache adjustment: {e_adj_count}.", extra={'org_group': org_name})
            logger.info(f"ADAPTIVE DELAY/PROCESSING: Using {num_repos_in_target} (cached, possibly adjusted) for target '{org_name}'.", extra={'org_group': org_name})

    if num_repos_in_target == 0:
        try:
            logger.info(f"ADAPTIVE DELAY/PROCESSING: Fetching live repository list for '{org_name}' to get count.", extra={'org_group': org_name})
            apply_dynamic_github_delay(cfg_obj, None, max_workers, is_graphql_call=False)
            live_repo_list_materialized = list(organization_obj_for_iteration.get_repos(type='all'))
            initial_live_count = len(live_repo_list_materialized)
            logger.info(f"ADAPTIVE DELAY/PROCESSING: Fetched {initial_live_count} live repositories for '{org_name}' before date filtering.", extra={'org_group': org_name})
            if repos_created_after_filter_date and live_repo_list_materialized:
                # (Date filtering logic for live_repo_list_materialized remains the same)
                filtered_live_repos = []
                skipped_legacy_count = 0
                for repo_stub_item in live_repo_list_materialized:
                    if not repo_stub_item.private: filtered_live_repos.append(repo_stub_item); continue
                    created_at_dt = repo_stub_item.created_at.replace(tzinfo=timezone.utc) if repo_stub_item.created_at else None
                    modified_at_dt = repo_stub_item.pushed_at.replace(tzinfo=timezone.utc) if repo_stub_item.pushed_at else None
                    if (created_at_dt and created_at_dt >= repos_created_after_filter_date) or \
                       (modified_at_dt and modified_at_dt >= repos_created_after_filter_date):
                        filtered_live_repos.append(repo_stub_item)
                    else: skipped_legacy_count += 1
                live_repo_list_materialized = filtered_live_repos
                if skipped_legacy_count > 0: logger.info(f"GitHub: Skipped {skipped_legacy_count} private legacy repos due to date filter.", extra={'org_group': org_name})
            num_repos_in_target = len(live_repo_list_materialized)
            logger.info(f"ADAPTIVE DELAY/PROCESSING: Using API count of {num_repos_in_target} (after date filter) for target '{org_name}'.", extra={'org_group': org_name})
        except Exception as e_live_count:
            logger.warning(f"GitHub: Error fetching live repository list for count: {e_live_count}. num_repos_in_target will be 0.", exc_info=True, extra={'org_group': org_name})
            num_repos_in_target = 0

    # --- Calculate inter-repo adaptive delay ---
    # (This logic remains the same)
    if cfg_obj and cfg_obj.ADAPTIVE_DELAY_ENABLED_ENV and num_repos_in_target > 0:
        if num_repos_in_target > cfg_obj.ADAPTIVE_DELAY_THRESHOLD_REPOS_ENV:
            excess_repos = num_repos_in_target - cfg_obj.ADAPTIVE_DELAY_THRESHOLD_REPOS_ENV
            scale_factor = 1 + (excess_repos / cfg_obj.ADAPTIVE_DELAY_THRESHOLD_REPOS_ENV)
            calculated_delay = cfg_obj.ADAPTIVE_DELAY_BASE_SECONDS_ENV * scale_factor
            inter_repo_adaptive_delay_per_repo = min(calculated_delay, cfg_obj.ADAPTIVE_DELAY_MAX_SECONDS_ENV)
        if inter_repo_adaptive_delay_per_repo > 0: logger.info(f"{ANSI_YELLOW}GitHub: INTER-REPO adaptive delay: {inter_repo_adaptive_delay_per_repo:.2f}s per repo.{ANSI_RESET}", extra={'org_group': org_name})
        elif num_repos_in_target > 0: logger.info(f"GitHub: Adaptive delay not applied for '{org_name}'.", extra={'org_group': org_name})
    # ... (other adaptive delay logging)

    processed_repo_list: List[Dict[str, Any]] = []
    repo_count_for_org_processed_or_submitted = 0
    skipped_by_date_filter_count = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_repo_name = {}
        try:
            iterable_repos = live_repo_list_materialized if live_repo_list_materialized is not None else organization_obj_for_iteration.get_repos(type='all')
            for repo_stub in iterable_repos: # repo_stub is a PyGithub Repository object (lazy loaded)
                with processed_counter_lock:
                    if debug_limit is not None and processed_counter[0] >= debug_limit:
                        logger.info(f"Global debug limit ({debug_limit}) reached. Stopping submissions for {org_name}.", extra={'org_group': org_name})
                        break
                    processed_counter[0] += 1
                
                # --- Apply REPOS_CREATED_AFTER_DATE filter ---
                # (This logic remains the same, applied to repo_stub)
                if repos_created_after_filter_date:
                    if repo_stub.private:
                        created_at_dt = repo_stub.created_at.replace(tzinfo=timezone.utc) if repo_stub.created_at else None
                        modified_at_dt = repo_stub.pushed_at.replace(tzinfo=timezone.utc) if repo_stub.pushed_at else None
                        if not ((created_at_dt and created_at_dt >= repos_created_after_filter_date) or \
                                (modified_at_dt and modified_at_dt >= repos_created_after_filter_date)):
                            with processed_counter_lock: processed_counter[0] -=1
                            skipped_by_date_filter_count += 1
                            continue
                        else: # Log if it passes
                            created_at_log_str = created_at_dt.strftime('%Y-%m-%d %H:%M:%S %Z') if created_at_dt else 'N/A'
                            modified_at_log_str = modified_at_dt.strftime('%Y-%m-%d %H:%M:%S %Z') if modified_at_dt else 'N/A'
                            log_message_parts = [f"GitHub: Private repo '{repo_stub.full_name}' included. "]
                            if (created_at_dt and created_at_dt >= repos_created_after_filter_date): log_message_parts.append(f"Created on ({created_at_log_str}).")
                            elif (modified_at_dt and modified_at_dt >= repos_created_after_filter_date): log_message_parts.append(f"Last modified on ({modified_at_log_str}).")
                            logger.info(" ".join(log_message_parts), extra={'org_group': org_name})


                repo_count_for_org_processed_or_submitted +=1
                future = executor.submit(
                    _process_single_github_repository,
                    repo_stub, # Pass the PyGithub repo stub
                    org_name=org_name,
                    token=token,
                    github_instance_url=github_instance_url, # For labor estimator if it needs REST
                    hours_per_commit=hours_per_commit,
                    cfg_obj=cfg_obj,
                    inter_repo_adaptive_delay_seconds=inter_repo_adaptive_delay_per_repo,
                    num_repos_in_target=num_repos_in_target,
                    # gql_client=gql_client_instance, # Don't pass shared client
                    graphql_endpoint_url_for_client=graphql_endpoint_url_for_threads, # Pass URL
                    previous_scan_cache=previous_scan_cache,
                    num_workers=max_workers
                )
                future_to_repo_name[future] = repo_stub.full_name
        
        except RateLimitExceededException as rle_iter: logger.error(f"GitHub API rate limit (PyGithub listing) for {org_name}. Details: {rle_iter}", extra={'org_group': org_name})
        except GithubException as gh_ex_iter: logger.error(f"GitHub API error (PyGithub listing) for {org_name}: {gh_ex_iter}.", extra={'org_group': org_name})
        except Exception as ex_iter: logger.error(f"Unexpected error (PyGithub listing) for {org_name}: {ex_iter}.", extra={'org_group': org_name})

        for future in as_completed(future_to_repo_name):
            repo_name_for_log = future_to_repo_name[future]
            try:
                repo_data_result = future.result()
                if repo_data_result:
                    if repo_data_result.get("processing_status") != "skipped_fork":
                        processed_repo_list.append(repo_data_result)
            except Exception as exc:
                logger.error(f"Repo {repo_name_for_log} generated an exception: {exc}", exc_info=True, extra={'org_group': org_name})
                processed_repo_list.append({"name": repo_name_for_log.split('/')[-1], "organization": org_name, "processing_error": f"Thread execution failed: {exc}"})

    logger.info(f"Finished processing for {repo_count_for_org_processed_or_submitted} repos from GitHub org: {org_name}. Collected {len(processed_repo_list)} results.", extra={'org_group': org_name})
    if repos_created_after_filter_date and skipped_by_date_filter_count > 0:
        logger.info(f"GitHub: Skipped {skipped_by_date_filter_count} private repos from '{org_name}' due to date filter.", extra={'org_group': org_name})

    return processed_repo_list

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Test configuration from environment variables
    test_gh_token = os.getenv("GITHUB_TOKEN_TEST")
    test_org_name_env = os.getenv("GITHUB_ORGS_TEST", "").split(',')[0].strip()
    test_ghes_url_env = os.getenv("GITHUB_ENTERPRISE_URL_TEST") # For GHES testing
    
    # Create a mock cfg_obj for testing if needed, or ensure your .env has test values
    class MockConfig:
        # Populate with relevant attributes your connector/delay logic might use from cfg_obj
        # For example:
        ADAPTIVE_DELAY_ENABLED_ENV = os.getenv("ADAPTIVE_DELAY_ENABLED", "False").lower() == "true"
        ADAPTIVE_DELAY_THRESHOLD_REPOS_ENV = int(os.getenv("ADAPTIVE_DELAY_THRESHOLD_REPOS", "100"))
        ADAPTIVE_DELAY_BASE_SECONDS_ENV = float(os.getenv("ADAPTIVE_DELAY_BASE_SECONDS", "0.1"))
        ADAPTIVE_DELAY_MAX_SECONDS_ENV = float(os.getenv("ADAPTIVE_DELAY_MAX_SECONDS", "2.0"))
        GITHUB_GRAPHQL_CALL_DELAY_SECONDS_ENV = float(os.getenv("GITHUB_GRAPHQL_CALL_DELAY_SECONDS", "0.25")) # Example
        # Add other relevant config attributes here if your test needs them
        AI_ENABLED_ENV = False # Example, adjust as needed for testing exemption processor path
        MAX_TOKENS_ENV = 1000
        AI_MAX_OUTPUT_TOKENS_ENV = 200
        AI_MODEL_NAME_ENV = "mock_model"
        AI_TEMPERATURE_ENV = 0.1


    mock_cfg_for_test = MockConfig()


    if not test_gh_token or is_placeholder_token(test_gh_token):
        logger.error("Test GitHub token (GITHUB_TOKEN_TEST) not found or is a placeholder in .env.")
    elif not test_org_name_env:
        logger.error("No GitHub organization found in GITHUB_ORGS_TEST in .env for testing.")
    else:
        instance_for_test = test_ghes_url_env or "public GitHub.com"
        logger.info(f"--- Testing GitHub Connector (GraphQL Refactored) for organization: {test_org_name_env} on instance: {instance_for_test} ---")
        counter = [0]
        counter_lock = threading.Lock()
        
        repositories = fetch_repositories(
            token=test_gh_token,
            org_name=test_org_name_env,
            processed_counter=counter,
            processed_counter_lock=counter_lock,
            debug_limit=None, # Set to a small number like 2 for quick testing
            github_instance_url=test_ghes_url_env,
            hours_per_commit=None, # Or a value like 0.5 to test labor hours
            max_workers=2, # Lower for focused testing
            cfg_obj=mock_cfg_for_test, # Pass the mock config
            previous_scan_output_file=None # No cache for direct test
        )

        if repositories:
            logger.info(f"Successfully fetched {len(repositories)} repositories.")
            for i, repo_info in enumerate(repositories[:min(len(repositories), 3)]): # Print first 3 or fewer
                logger.info(f"--- Repository {i+1} ({repo_info.get('name')}) ---")
                logger.info(f"  Repo ID: {repo_info.get('repo_id')}")
                logger.info(f"  Name: {repo_info.get('name')}")
                logger.info(f"  Org: {repo_info.get('organization')}")
                logger.info(f"  Description: {repo_info.get('description')}")
                logger.info(f"  Visibility: {repo_info.get('repositoryVisibility')}")
                logger.info(f"  Archived: {repo_info.get('archived')}")
                logger.info(f"  Languages: {repo_info.get('languages')}")
                logger.info(f"  Topics (tags): {repo_info.get('tags')}")
                logger.info(f"  Git Tags (_api_tags): {repo_info.get('_api_tags')}")
                logger.info(f"  Last Commit SHA: {repo_info.get('lastCommitSHA')}")
                logger.info(f"  Permissions: {repo_info.get('permissions')}")
                logger.info(f"  Contact: {repo_info.get('contact')}")
                if "processing_error" in repo_info:
                    logger.error(f"  Processing Error: {repo_info['processing_error']}")
            if len(repositories) > 3:
                logger.info(f"... and {len(repositories)-3} more repositories.")
        else:
            logger.warning("No repositories fetched or an error occurred.")
        logger.info(f"Total repositories processed according to counter: {counter[0]}")
