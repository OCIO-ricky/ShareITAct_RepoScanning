# clients/github_connector.py
"""
GitHub Connector for Share IT Act Repository Scanning Tool.r

This module is responsible for fetching repository data from GitHub,
including metadata, README content, CODEOWNERS files, topics, and tags.
It interacts with the GitHub API via the PyGithub library.
"""

import os
import logging
import time 
import base64 # For decoding README content
import threading # For locks
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Optional, Any
from utils.delay_calculator import calculate_dynamic_delay # Import the calculator
from utils.caching import load_previous_scan_data, PLATFORM_CACHE_CONFIG
from datetime import timezone, datetime
from utils.dateparse import parse_repos_created_after_date # Import the new utility
from utils.fetch_utils import (
    fetch_optional_content_with_retry,
    FETCH_ERROR_FORBIDDEN, FETCH_ERROR_NOT_FOUND, FETCH_ERROR_EMPTY_REPO_API,
    FETCH_ERROR_API_ERROR, FETCH_ERROR_UNEXPECTED
)
from utils.labor_hrs_estimator import analyze_github_repo_sync # Import the estimator

from github import Github, GithubException, UnknownObjectException, RateLimitExceededException

# ANSI escape codes for coloring output
ANSI_YELLOW = "\x1b[33;1m"
ANSI_RED = "\x1b[31;1m"
ANSI_RESET = "\x1b[0m"   # Reset to default color

# Attempt to import the exemption processor
try:
    from utils import exemption_processor
except ImportError:
    logging.getLogger(__name__).error(
        "Failed to import exemption_processor from utils. "
        "Exemption processing will be skipped by the GitHub connector (using mock)."
    )
    class MockExemptionProcessor:
        def process_repository_exemptions(self, repo_data: Dict[str, Any], default_org_identifiers: Optional[List[str]] = None) -> Dict[str, Any]:
            repo_data.setdefault('_status_from_readme', None)
            repo_data.setdefault('_private_contact_emails', [])
            repo_data.setdefault('contact', {})
            repo_data.setdefault('permissions', {"usageType": "openSource"})
            repo_data.pop('readme_content', None)
            repo_data.pop('_codeowners_content', None)
            return repo_data
    exemption_processor = MockExemptionProcessor()

logger = logging.getLogger(__name__)
# DONT CHANGE THIS PLACEHOLDER
PLACEHOLDER_GITHUB_TOKEN = "YOUR_GITHUB_PAT"

# --- Constants for the new utility ---
GITHUB_EXCEPTION_MAP = {
    'forbidden_exception': lambda e: isinstance(e, GithubException) and hasattr(e, 'status') and e.status == 403,
    'not_found_exception': UnknownObjectException,
    'empty_repo_check_func': lambda e: (
        isinstance(e, GithubException) and
        e.status == 404 and # PyGithub often uses 404 for empty repo on get_contents
        isinstance(e.data, dict) and
        e.data.get('message') == 'This repository is empty.'
    ),
    'generic_platform_exception': GithubException
}
MAX_QUICK_CONTENT_RETRIES = 2  # Number of quick retries for 403 on optional content
QUICK_CONTENT_RETRY_DELAY_SECONDS = 3 # Delay between these quick retries


def apply_dynamic_github_delay(cfg_obj: Optional[Any], num_repos_in_target: Optional[int], num_workers: int = 1):
    """
    Calculates and applies a dynamic delay based on the number of repositories in the target
    and the number of concurrent workers.
    This is a synchronous sleep for PyGithub calls.
    """
    delay_seconds = 0.0
    if cfg_obj:
        base_delay = float(getattr(cfg_obj, 'GITHUB_POST_API_CALL_DELAY_SECONDS_ENV', os.getenv("GITHUB_POST_API_CALL_DELAY_SECONDS", "0.0")))
        threshold = int(getattr(cfg_obj, 'DYNAMIC_DELAY_THRESHOLD_REPOS_ENV', os.getenv("DYNAMIC_DELAY_THRESHOLD_REPOS", "100")))
        scale = float(getattr(cfg_obj, 'DYNAMIC_DELAY_SCALE_FACTOR_ENV', os.getenv("DYNAMIC_DELAY_SCALE_FACTOR", "1.5")))
        max_d = float(getattr(cfg_obj, 'DYNAMIC_DELAY_MAX_SECONDS_ENV', os.getenv("DYNAMIC_DELAY_MAX_SECONDS", "1.0")))
      
      ## DEBUG:   print(f"{ANSI_YELLOW}num_repos_in_target: {num_repos_in_target} ")
        delay_seconds = calculate_dynamic_delay(
            base_delay_seconds=base_delay,
            num_items=num_repos_in_target if num_repos_in_target is not None and num_repos_in_target > 0 else None,
            threshold_items=threshold, 
            scale_factor=scale, 
            max_delay_seconds=max_d,
            num_workers=num_workers  # Pass the number of workers
        )
  
    if delay_seconds > 0:
        logger.debug(f"Applying SYNC dynamic GitHub API call delay: {delay_seconds:.2f}s "
                     f"(based on target size: {num_repos_in_target}, workers: {num_workers})")
        time.sleep(delay_seconds)

def is_placeholder_token(token: Optional[str]) -> bool:
    """Checks if the GitHub token is missing or a known placeholder."""
    return not token or token == PLACEHOLDER_GITHUB_TOKEN

def _get_readme_details_pygithub(repo_obj, cfg_obj: Optional[Any], num_repos_in_target: Optional[int], num_workers: int = 1) -> tuple[Optional[str], Optional[str]]:
    """
    Fetches and decodes the README content and its HTML URL.
    Tries common README filenames.
    Returns: (content, url, is_empty_repo_error_occurred)
    """
    # Wrapper for the dynamic delay to be passed to the utility
    def _github_dynamic_delay_wrapper():
        apply_dynamic_github_delay(cfg_obj, num_repos_in_target, num_workers)

    common_readme_names = ["README.md", "README.txt", "README", "readme.md"]
    for readme_name in common_readme_names:
        fetch_lambda = lambda: repo_obj.get_contents(readme_name)

        raw_file_object, error_type = fetch_optional_content_with_retry(
            fetch_callable=fetch_lambda,
            content_description=f"README '{readme_name}'",
            repo_identifier=repo_obj.full_name,
            platform_exception_map=GITHUB_EXCEPTION_MAP,
            max_quick_retries=MAX_QUICK_CONTENT_RETRIES,
            quick_retry_delay_seconds=QUICK_CONTENT_RETRY_DELAY_SECONDS,
            logger_instance=logger,
            dynamic_delay_func=_github_dynamic_delay_wrapper
        )

        if error_type == FETCH_ERROR_EMPTY_REPO_API:
            return None, None, True # Signal empty repo
        if error_type == FETCH_ERROR_NOT_FOUND:
            continue # Try next readme name
        if error_type in [FETCH_ERROR_FORBIDDEN, FETCH_ERROR_API_ERROR, FETCH_ERROR_UNEXPECTED]:
            logger.error(f"Stopping README fetch for {repo_obj.full_name} due to error: {error_type}")
            return None, None, False # Indicate error, not necessarily empty repo
        
        if raw_file_object: # Success from fetch_optional_content_with_retry
            try:
                readme_content_bytes = base64.b64decode(raw_file_object.content)
                readme_content_str = readme_content_bytes.decode('utf-8')
            except UnicodeDecodeError:
                try:
                    readme_content_str = readme_content_bytes.decode('latin-1')
                except Exception:
                    readme_content_str = readme_content_bytes.decode('utf-8', errors='ignore')
            readme_url = raw_file_object.html_url
            logger.debug(f"Successfully fetched README '{readme_name}' (URL: {readme_url}) for {repo_obj.full_name}")
            return readme_content_str, readme_url, False
        # If raw_file_object is None and no critical error_type stopped us, it implies not found for this name

    logger.debug(f"No common README file found for {repo_obj.full_name}")
    return None, None, False

def _get_codeowners_content_pygithub(repo_obj, cfg_obj: Optional[Any], num_repos_in_target: Optional[int], num_workers: int = 1) -> Optional[str]:
    """
    Fetches CODEOWNERS content from standard locations.
    Returns: Tuple[Optional[str], bool] where bool indicates if an empty repo signal was encountered.
    """
    # Wrapper for the dynamic delay
    def _github_dynamic_delay_wrapper():
        apply_dynamic_github_delay(cfg_obj, num_repos_in_target, num_workers)

    codeowners_locations = ["CODEOWNERS", ".github/CODEOWNERS", "docs/CODEOWNERS"]
    for location in codeowners_locations:
        fetch_lambda = lambda: repo_obj.get_contents(location)

        raw_file_object, error_type = fetch_optional_content_with_retry(
            fetch_callable=fetch_lambda,
            content_description=f"CODEOWNERS from '{location}'",
            repo_identifier=repo_obj.full_name,
            platform_exception_map=GITHUB_EXCEPTION_MAP,
            max_quick_retries=MAX_QUICK_CONTENT_RETRIES,
            quick_retry_delay_seconds=QUICK_CONTENT_RETRY_DELAY_SECONDS,
            logger_instance=logger,
            dynamic_delay_func=_github_dynamic_delay_wrapper
        )
        if error_type == FETCH_ERROR_EMPTY_REPO_API:
            return None, True # Signal empty repo
        if error_type == FETCH_ERROR_NOT_FOUND:
            continue # Try next location
        if error_type in [FETCH_ERROR_FORBIDDEN, FETCH_ERROR_API_ERROR, FETCH_ERROR_UNEXPECTED]:
            logger.error(f"Stopping CODEOWNERS fetch for {repo_obj.full_name} due to error: {error_type}")
            return None, False # Indicate error, not necessarily empty repo
        
        if raw_file_object: # Success
            codeowners_content = raw_file_object.decoded_content.decode('utf-8', errors='replace')
            logger.debug(f"Successfully fetched CODEOWNERS from '{location}' for {repo_obj.full_name}")
            return codeowners_content, False

    logger.debug(f"No CODEOWNERS file found in standard locations for {repo_obj.full_name}")
    return None, False

def _fetch_tags_pygithub(repo_obj, cfg_obj: Optional[Any], num_repos_in_target: Optional[int], num_workers: int = 1) -> List[str]:
    tag_names = []
    try:
        logger.debug(f"Fetching tags for repo: {repo_obj.full_name}")
        apply_dynamic_github_delay(cfg_obj, num_repos_in_target, num_workers)# Apply delay BEFORE the call
        tags = repo_obj.get_tags()
        tag_names = [tag.name for tag in tags if tag.name]
        logger.debug(f"Found {len(tag_names)} tags for {repo_obj.full_name}")
    except RateLimitExceededException:
        logger.error(f"{ANSI_YELLOW}GitHub API Rate limit exceeded while fetching tags for {repo_obj.full_name}. Skipping tags for this repo.{ANSI_RESET}")
    except GithubException as e:
        logger.error(f"{ANSI_RED}GitHub API warning fetching tags for {repo_obj.full_name}: {e.status} {ANSI_RESET}{getattr(e, 'data', str(e))}", exc_info=False)
    except Exception as e: # Catch other potential errors like network issues during this specific call
        logger.error(f"{ANSI_RED}Unexpected error fetching tags for {repo_obj.full_name}: {e}{ANSI_RESET}", exc_info=True)
    return tag_names

def _process_single_github_repository(
    repo, # PyGithub Repository object
    org_name: str,
    token: Optional[str], 
    github_instance_url: Optional[str],
    hours_per_commit: Optional[float],
    cfg_obj: Any, # Pass the Config object
    inter_repo_adaptive_delay_seconds: float, # Inter-repository adaptive delay
    num_repos_in_target: Optional[int], # Pass the count for dynamic delay
    # --- Parameters for Caching ---
    previous_scan_cache: Dict[str, Dict],
    current_commit_sha: Optional[str],
    num_workers: int = 1  # Add this parameter
) -> Dict[str, Any]:
    """
    Processes a single GitHub repository to extract its metadata. # KEEP
    This function is intended to be run in a separate thread.

    Args:
        repo: The PyGithub Repository object.
        org_name: The name of the GitHub organization (owner of the repo).
        token: GitHub PAT for operations like labor hours estimation.
        github_instance_url: Base URL for GHES.
        hours_per_commit: Optional rate for labor hours estimation.
        cfg_obj: The configuration object from generate_codejson.py.
        inter_repo_adaptive_delay_seconds: Calculated inter-repo delay to sleep after processing.
        num_repos_in_target: Total number of repos in the current org, for dynamic delay calculation.
        previous_scan_cache: Dictionary of previously scanned repositories' data.
        current_commit_sha: The current commit SHA of the repository's default branch.

    Returns:
        A dictionary containing processed metadata for the repository.
    """
    repo_full_name = repo.full_name
    repo_data: Dict[str, Any] = {"name": repo.name, "organization": org_name}
    github_cache_config = PLATFORM_CACHE_CONFIG["github"] # Get GitHub specific cache keys
    repo_id_str = str(repo.id) # Use the live repository's ID as the key
    # --- Caching Logic ---
    if current_commit_sha: # Only attempt cache hit if we have a current SHA to compare
        cached_repo_entry = previous_scan_cache.get(repo_id_str)
        if cached_repo_entry:
            cached_commit_sha = cached_repo_entry.get(github_cache_config["commit_sha_field"])
            if cached_commit_sha and current_commit_sha == cached_commit_sha:
                logger.info(f"CACHE HIT: GitHub repo '{repo_full_name}' (ID: {repo_id_str}) has not changed. Using cached data.")
                
                # Start with the cached data
                repo_data_to_process = cached_repo_entry.copy()
                # Ensure the current (and matching) SHA is in the data for consistency
                repo_data_to_process[github_cache_config["commit_sha_field"]] = current_commit_sha
                
                # Re-process exemptions to apply current logic/AI models, even on cached data
                # Note: privateID mapping happens in generate_codejson.py after this.
                if cfg_obj:
                    repo_data_to_process = exemption_processor.process_repository_exemptions(
                        repo_data_to_process, default_org_identifiers=[org_name],
                        ai_is_enabled_from_config=cfg_obj.AI_ENABLED_ENV, ai_model_name_from_config=cfg_obj.AI_MODEL_NAME_ENV,
                        ai_temperature_from_config=cfg_obj.AI_TEMPERATURE_ENV, ai_max_output_tokens_from_config=cfg_obj.AI_MAX_OUTPUT_TOKENS_ENV,
                        ai_max_input_tokens_from_config=cfg_obj.MAX_TOKENS_ENV)
                return repo_data_to_process # Return cached and re-processed data
    logger.info(f"No SHA: Processing repository: {repo_full_name} (ID: {repo_id_str}) with full data fetch.")

    try:
        if repo.fork:
            logger.info(f"Skipping forked repository: {repo_full_name}")
            repo_data["processing_status"] = "skipped_fork"
            return repo_data

        # --- Early check for problematic repository (empty or no default branch access) ---
        is_problematic_for_content_fetch = False
        problem_reason = ""

        if repo.size == 0:
            is_problematic_for_content_fetch = True
            problem_reason = "size is 0"
        elif not repo.default_branch: # Check if the repo_stub itself indicates no default branch
            is_problematic_for_content_fetch = True
            problem_reason = "no default_branch attribute on repo object"
        elif current_commit_sha is None: # Implies get_branch(repo.default_branch) failed
            is_problematic_for_content_fetch = True
            problem_reason = f"current_commit_sha is None (default branch '{repo.default_branch}' likely not found or empty)"

        if is_problematic_for_content_fetch:
            logger.info(f"Repository {repo_full_name} is problematic for content fetching ({problem_reason}). Skipping detailed content fetching.")
            repo_data['_is_empty_repo'] = True
            # Populate minimal required fields and then return
            repo_data.update({
                "description": repo.description or "", 
                "repositoryURL": repo.html_url, 
                "vcs": "git",
                "repositoryVisibility": "public" if not repo.private else "private", # Basic visibility
                "status": "development", 
                "version": "N/A", 
                "laborHours": 0, 
                "languages": [], 
                "tags": [],
                "date": {"created": repo.created_at.replace(tzinfo=timezone.utc).isoformat() if repo.created_at else None,
                         "lastModified": repo.pushed_at.replace(tzinfo=timezone.utc).isoformat() if repo.pushed_at else None},
                "permissions": {"usageType": "openSource", 
                                "exemptionText": None, 
                                "licenses": []},
                "contact": {}, 
                "repo_id": repo.id, 
#                "fullName": repo_full_name, 
                "archived": repo.archived
            })
            if current_commit_sha: # Should be None if truly empty, but for consistency
                repo_data[github_cache_config["commit_sha_field"]] = current_commit_sha
            
            # Still run exemption processor for potential name/description based rules
            if cfg_obj:
                repo_data = exemption_processor.process_repository_exemptions(
                    repo_data, default_org_identifiers=[org_name],
                    ai_is_enabled_from_config=cfg_obj.AI_ENABLED_ENV, ai_model_name_from_config=cfg_obj.AI_MODEL_NAME_ENV,
                    ai_temperature_from_config=cfg_obj.AI_TEMPERATURE_ENV, ai_max_output_tokens_from_config=cfg_obj.AI_MAX_OUTPUT_TOKENS_ENV,
                    ai_max_input_tokens_from_config=cfg_obj.MAX_TOKENS_ENV)
            return repo_data

        created_at_dt = repo.created_at.replace(tzinfo=timezone.utc) if repo.created_at else None
        pushed_at_dt = repo.pushed_at.replace(tzinfo=timezone.utc) if repo.pushed_at else None 
        updated_at_dt = repo.updated_at.replace(tzinfo=timezone.utc) if repo.updated_at else None

        repo_visibility = "public" 
        if repo.private:
            repo_visibility = "private"
        if hasattr(repo, 'visibility') and repo.visibility: 
            if repo.visibility.lower() in ["public", "private", "internal"]:
                 repo_visibility = repo.visibility.lower()

        all_languages_list = []
        try:
            apply_dynamic_github_delay(cfg_obj, num_repos_in_target, num_workers) # Apply delay BEFORE the call
            languages_dict = repo.get_languages()
            if not languages_dict and repo.size == 0:
                logger.info(f"Repository {repo_full_name} has no languages and size is 0, likely empty.")
            elif languages_dict:
                all_languages_list = list(languages_dict.keys())
        except GithubException as lang_err:
            if lang_err.status == 404 and isinstance(lang_err.data, dict) and lang_err.data.get('message') == 'This repository is empty.':
                logger.info(f"Repository {repo_full_name} is confirmed empty by API (get_languages).")
                repo_data['_is_empty_repo'] = True
            else:
                logger.warning(f"Could not fetch languages for {repo_full_name}: {lang_err.status} {getattr(lang_err, 'data', str(lang_err))}", exc_info=False)
        except Exception as lang_err:
            logger.warning(f"Could not fetch languages for {repo_full_name}: {lang_err}", exc_info=False)
        
        licenses_list = []
        if repo.license and hasattr(repo.license, 'spdx_id') and repo.license.spdx_id and repo.license.spdx_id.lower() != "noassertion":
            license_entry = {"spdxID": repo.license.spdx_id}
            if hasattr(repo.license, 'name') and repo.license.name:
                license_entry["name"] = repo.license.name
            licenses_list.append(license_entry)
        
        readme_content_str, readme_html_url, readme_empty_repo_error = _get_readme_details_pygithub(repo, cfg_obj, num_repos_in_target, num_workers)
        codeowners_content_str, codeowners_empty_repo_error = _get_codeowners_content_pygithub(repo, cfg_obj, num_repos_in_target, num_workers)

        if not repo_data.get('_is_empty_repo', False):
            repo_data['_is_empty_repo'] = readme_empty_repo_error or codeowners_empty_repo_error

        apply_dynamic_github_delay(cfg_obj, num_repos_in_target, num_workers) # Delay before get_topics
        repo_topics = repo.get_topics()
        repo_git_tags = _fetch_tags_pygithub(repo, cfg_obj, num_repos_in_target)
        
        repo_data.update({
            "description": repo.description or "",
            "repositoryURL": repo.html_url,
            "homepageURL": repo.homepage or "", 
            "downloadURL": None, 
            "readme_url": readme_html_url, 
            "vcs": "git",
            "repositoryVisibility": repo_visibility,
            "status": "development", 
            "version": "N/A",      
            "laborHours": 0,       
            "languages": all_languages_list,
            "tags": repo_topics,
            "date": {
                "created": created_at_dt.isoformat() if created_at_dt else None,
                "lastModified": pushed_at_dt.isoformat() if pushed_at_dt else (updated_at_dt.isoformat() if updated_at_dt else None),
            },
            "permissions": {
                "usageType": None, # Initialize to None for full data fetch
                "exemptionText": None,
                "licenses": licenses_list
            },
            "contact": {}, 
            "contractNumber": None, 
            "readme_content": readme_content_str,
            "_codeowners_content": codeowners_content_str,
            "repo_id": repo.id, 
#            "fullName": repo_full_name, 
            "_api_tags": repo_git_tags, 
            "archived": repo.archived,
        })
        repo_data.setdefault('_is_empty_repo', False)
         # Store the current commit SHA for the next scan's cache, if available
        if current_commit_sha:
            repo_data[github_cache_config["commit_sha_field"]] = current_commit_sha
       
        if hours_per_commit is not None:
            logger.debug(f"Estimating labor hours for GitHub repo: {repo.full_name}")
            try:
                labor_df = analyze_github_repo_sync(
                    owner=org_name,
                    repo=repo.name,
                    token=token,
                    hours_per_commit=hours_per_commit,
                    github_api_url=github_instance_url or "https://api.github.com",
                    session=None,
                    cfg_obj=cfg_obj,
                    num_repos_in_target=num_repos_in_target,
                    is_empty_repo=repo_data.get('_is_empty_repo', False),
                    number_of_workers=num_workers
                )
                if not labor_df.empty:
                    repo_data["laborHours"] = round(float(labor_df["EstimatedHours"].sum()), 2)
                    logger.info(f"Estimated labor hours for {repo.full_name}: {repo_data['laborHours']}")
                else:
                    repo_data["laborHours"] = 0.0
            except Exception as e_lh:
                logger.warning(f"Could not estimate labor hours for {repo.full_name}: {e_lh}", exc_info=True)
                repo_data["laborHours"] = 0.0

        if cfg_obj:
            repo_data = exemption_processor.process_repository_exemptions(
                repo_data,
                default_org_identifiers=[org_name],
                ai_is_enabled_from_config=cfg_obj.AI_ENABLED_ENV,
                ai_model_name_from_config=cfg_obj.AI_MODEL_NAME_ENV,
                ai_temperature_from_config=cfg_obj.AI_TEMPERATURE_ENV,
                ai_max_output_tokens_from_config=cfg_obj.AI_MAX_OUTPUT_TOKENS_ENV,
                ai_max_input_tokens_from_config=cfg_obj.MAX_TOKENS_ENV
            )
        else:
            logger.warning(f"cfg_obj not provided to _process_single_github_repository for {repo_full_name}. Exemption processor will use its internal defaults/env vars for AI settings.")
            repo_data = exemption_processor.process_repository_exemptions(
                repo_data, default_org_identifiers=[org_name]
            )

        if inter_repo_adaptive_delay_seconds > 0:
            logger.debug(f"GitHub repo {repo_full_name}: Applying INTER-REPO adaptive delay of {inter_repo_adaptive_delay_seconds:.2f}s")
            time.sleep(inter_repo_adaptive_delay_seconds)
        
        return repo_data

    except RateLimitExceededException as rle_repo:
        logger.error(f"GitHub API rate limit exceeded processing repo {repo_full_name}. Details: {rle_repo}")
        repo_data["processing_error"] = f"GitHub API Rate Limit Error: {rle_repo}"
        return repo_data
    except GithubException as gh_err_repo:
        logger.error(f"GitHub API error processing repo {repo_full_name}: {gh_err_repo.status} {getattr(gh_err_repo, 'data', str(gh_err_repo))}.", exc_info=False)
        repo_data["processing_error"] = f"GitHub API Error: {gh_err_repo.status}"
        return repo_data
    except Exception as e_repo:
        logger.error(f"Unexpected error processing repo {repo_full_name}: {e_repo}.", exc_info=True)
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
    previous_scan_output_file: Optional[str] = None # For caching
) -> list[dict]:
    """
    Fetches repository details from a specific GitHub organization.
    
    Args:
        token: The GitHub Personal Access Token.
        org_name: The name of the GitHub organization to scan.
        processed_counter: Mutable list to track processed repositories for debug limit.
        processed_counter_lock: Lock for safely updating processed_counter.
        debug_limit: Optional global limit for repositories to process.
        github_instance_url: The base URL of the GitHub instance. Defaults to https://api.github.com if None.
        hours_per_commit: Optional factor to estimate labor hours based on commit count.
        max_workers: Number of concurrent worker threads for repository processing.
                     This affects rate limiting calculations.
        cfg_obj: Configuration object containing settings for API calls, delays, and exemption processing.
        previous_scan_output_file: Path to previous scan results for caching optimization.
    
    Returns:
        A list of dictionaries, each containing processed metadata for a repository.
    """
    instance_msg = f"GitHub instance: {github_instance_url}" if github_instance_url else "public GitHub.com"
    logger.info(f"Attempting to fetch repositories CONCURRENTLY for GitHub organization: {org_name} on {instance_msg} (max_workers: {max_workers})")

    if is_placeholder_token(token):
        logger.error("GitHub token is a placeholder or missing. Cannot fetch repositories.")
        return []

    # Parse the REPOS_CREATED_AFTER_DATE from cfg_obj
    repos_created_after_filter_date: Optional[datetime] = None
    if cfg_obj and hasattr(cfg_obj, 'REPOS_CREATED_AFTER_DATE'):
        repos_created_after_filter_date = parse_repos_created_after_date(cfg_obj.REPOS_CREATED_AFTER_DATE, logger)

    # --- Load Previous Scan Data for Caching ---
    previous_scan_cache: Dict[str, Dict] = {}
    if previous_scan_output_file:
        # ... (previous_scan_cache loading logic remains the same)
        logger.info(f"Attempting to load previous GitHub scan data for '{org_name}' from: {previous_scan_output_file}")
        # Ensure previous_scan_cache is a dict, even if load_previous_scan_data returns None or wrong type
        loaded_cache = load_previous_scan_data(previous_scan_output_file, "github")
        if isinstance(loaded_cache, dict):
            previous_scan_cache = loaded_cache
        else:
            logger.warning(f"CACHE: load_previous_scan_data did not return a dict for {previous_scan_output_file}. Cache will be empty.")
    else:
        logger.info(f"No previous scan output file provided for GitHub org '{org_name}'. Full scan for all repos in this org.")

    try:
        gh_url_for_org_processing = None
        if github_instance_url:
            gh_url_for_org_processing = github_instance_url.rstrip('/') + "/api/v3" if not github_instance_url.endswith("/api/v3") else github_instance_url
 
        ssl_verify_flag = True # Default to True (verify SSL)
        disable_ssl_env = os.getenv("DISABLE_SSL_VERIFICATION", "false").lower()
        if disable_ssl_env == "true":
            ssl_verify_flag = False
            logger.warning(f"{ANSI_RED}SECURITY WARNING: SSL certificate verification is DISABLED for GitHub connections due to DISABLE_SSL_VERIFICATION=true.{ANSI_RESET}")
            logger.warning(f"{ANSI_YELLOW}This should ONLY be used for trusted internal environments. Do NOT use in production with public-facing services.{ANSI_RESET}")
        
        # Ensure base_url is always a string, defaulting to public GitHub API if gh_url_for_org_processing is None
        effective_base_url_for_gh = gh_url_for_org_processing if gh_url_for_org_processing else "https://api.github.com"
        gh = Github(login_or_token=token, base_url=effective_base_url_for_gh, verify=ssl_verify_flag, timeout=30)
        # Fetch the organization object once. It will be used for iterating repos.
        # The num_repos_in_target for this initial call to apply_dynamic_github_delay is not yet known from cache/live.
        # Passing None will result in base_delay or 0 if base_delay is 0.
        apply_dynamic_github_delay(cfg_obj, None, max_workers) 
        organization_obj_for_iteration = gh.get_organization(org_name)
        logger.info(f"Successfully configured GitHub client for organization: {org_name}.")
    except Exception as e:
        logger.critical(f"Failed to initialize GitHub client for org '{org_name}': {e}", exc_info=True)
        return []

    # --- Determine num_repos_in_target for adaptive delay and dynamic intra-repo delays ---
    num_repos_in_target = 0 
    inter_repo_adaptive_delay_per_repo = 0.0
    live_repo_list_materialized = None # To store the live list if fetched for count

    cached_repo_count_for_target = 0
    if previous_scan_cache: # Check if cache was loaded and is not empty
        github_id_field = PLATFORM_CACHE_CONFIG.get("github", {}).get("id_field", "repo_id")
        valid_cached_repos = [
            r_data for r_id, r_data in previous_scan_cache.items() 
            if isinstance(r_data, dict) and r_data.get(github_id_field) is not None
        ]
        cached_repo_count_for_target = len(valid_cached_repos)
        if cached_repo_count_for_target > 0: # If we have a valid cached count
            logger.info(f"CACHE: Found {cached_repo_count_for_target} valid repos in cache for '{org_name}'.")
            num_repos_in_target = cached_repo_count_for_target

            # If REPOS_CREATED_AFTER_DATE filter is active, adjust num_repos_in_target
            # to account for potentially additional private repos that may have been modified after the filter date.
            if repos_created_after_filter_date and cfg_obj and hasattr(cfg_obj, 'ADAPTIVE_DELAY_CACHE_MODIFIED_FACTOR_ENV'):
                adjustment_factor = float(cfg_obj.ADAPTIVE_DELAY_CACHE_MODIFIED_FACTOR_ENV)
                try:
                    # Fetch total live count to compare (this is an API call)
                    apply_dynamic_github_delay(cfg_obj, None, max_workers) # Delay before this potentially significant list operation
                    total_live_repos_for_adjustment = organization_obj_for_iteration.get_repos(type='all').totalCount
                    
                    if total_live_repos_for_adjustment > cached_repo_count_for_target:
                        diff_count = total_live_repos_for_adjustment - cached_repo_count_for_target
                        additional_repos_estimate = int(diff_count * adjustment_factor)
                        if additional_repos_estimate > 0:
                            num_repos_in_target += additional_repos_estimate
                            logger.info(f"ADAPTIVE DELAY/PROCESSING: Adjusted target estimate for '{org_name}' by {additional_repos_estimate} (due to date filter & potential modifications). New estimate: {num_repos_in_target}.")
                        else:
                            logger.info(f"ADAPTIVE DELAY/PROCESSING: Calculated additional repo estimate is {additional_repos_estimate} for '{org_name}'. No change to target estimate from adjustment.")
                    else:
                        logger.info(f"ADAPTIVE DELAY/PROCESSING: Live repo count ({total_live_repos_for_adjustment}) is not greater than cached count ({cached_repo_count_for_target}) for '{org_name}'. No adjustment made.")
                except Exception as e_adj_count:
                    logger.warning(f"GitHub: Error fetching total live repo count for cache adjustment for '{org_name}': {e_adj_count}. Using unadjusted cached count.")
            logger.info(f"ADAPTIVE DELAY/PROCESSING: Using {num_repos_in_target} (cached, possibly adjusted) as total items estimate for target '{org_name}'.")

    if num_repos_in_target == 0: # If cache was empty or not used
        try:
            logger.info(f"ADAPTIVE DELAY/PROCESSING: Cache empty or not used for count. Fetching live repository list for '{org_name}' to get count.")
            apply_dynamic_github_delay(cfg_obj, None, max_workers) # Delay before get_repos list
            live_repo_list_materialized = list(organization_obj_for_iteration.get_repos(type='all'))
            initial_live_count = len(live_repo_list_materialized)
            logger.info(f"ADAPTIVE DELAY/PROCESSING: Fetched {initial_live_count} live repositories for '{org_name}' before date filtering.")

            # --- Apply REPOS_CREATED_AFTER_DATE filter to live_repo_list_materialized ---
            if repos_created_after_filter_date and live_repo_list_materialized:
                filtered_live_repos = []
                skipped_legacy_count = 0
                for repo_stub_item in live_repo_list_materialized:
                    if not repo_stub_item.private: # Public repos always pass this specific filter
                        filtered_live_repos.append(repo_stub_item)
                        continue
                    # Private repo, check dates
                    created_at_dt = repo_stub_item.created_at.replace(tzinfo=timezone.utc) if repo_stub_item.created_at else None
                    modified_at_dt = repo_stub_item.pushed_at.replace(tzinfo=timezone.utc) if repo_stub_item.pushed_at else None
                    
                    if (created_at_dt and created_at_dt >= repos_created_after_filter_date) or \
                       (modified_at_dt and modified_at_dt >= repos_created_after_filter_date):
                        filtered_live_repos.append(repo_stub_item)
                    else:
                        skipped_legacy_count += 1
                
                live_repo_list_materialized = filtered_live_repos # Update with filtered list
                if skipped_legacy_count > 0:
                    logger.info(f"GitHub: Skipped {skipped_legacy_count} private legacy repositories from '{org_name}' due to REPOS_CREATED_AFTER_DATE filter ('{repos_created_after_filter_date.strftime('%Y-%m-%d')}') before full processing.")
            
            num_repos_in_target = len(live_repo_list_materialized) # Count after filtering
            logger.info(f"ADAPTIVE DELAY/PROCESSING: Using API count of {num_repos_in_target} (after date filter) as total items estimate for target '{org_name}'.")
        except Exception as e_live_count:
            logger.warning(f"GitHub: Error fetching live repository list for '{org_name}' to get count: {e_live_count}. num_repos_in_target will be 0.", exc_info=True)
            num_repos_in_target = 0 # Fallback if live counting fails

    # --- Calculate inter-repo adaptive delay if enabled ---
    if cfg_obj and cfg_obj.ADAPTIVE_DELAY_ENABLED_ENV and num_repos_in_target > 0:
        if num_repos_in_target > cfg_obj.ADAPTIVE_DELAY_THRESHOLD_REPOS_ENV:
            excess_repos = num_repos_in_target - cfg_obj.ADAPTIVE_DELAY_THRESHOLD_REPOS_ENV
            scale_factor = 1 + (excess_repos / cfg_obj.ADAPTIVE_DELAY_THRESHOLD_REPOS_ENV) 
            calculated_delay = cfg_obj.ADAPTIVE_DELAY_BASE_SECONDS_ENV * scale_factor
            inter_repo_adaptive_delay_per_repo = min(calculated_delay, cfg_obj.ADAPTIVE_DELAY_MAX_SECONDS_ENV)
        if inter_repo_adaptive_delay_per_repo > 0:
            logger.info(f"{ANSI_YELLOW}GitHub: INTER-REPO adaptive delay calculated for '{org_name}': {inter_repo_adaptive_delay_per_repo:.2f}s per repo (based on {num_repos_in_target} repos).{ANSI_RESET}")
        elif num_repos_in_target > 0 : 
            logger.info(f"GitHub: Adaptive delay not applied for '{org_name}' (num_repos: {num_repos_in_target}, threshold: {cfg_obj.ADAPTIVE_DELAY_THRESHOLD_REPOS_ENV}).")
    elif cfg_obj and cfg_obj.ADAPTIVE_DELAY_ENABLED_ENV and num_repos_in_target == 0:
        logger.info(f"GitHub: Adaptive delay enabled but num_repos_in_target is 0 for '{org_name}'. No inter-repo adaptive delay will be applied.")
    elif cfg_obj: # Adaptive delay is configured but disabled
        logger.info(f"GitHub: Adaptive delay is disabled by configuration for '{org_name}'.")

    processed_repo_list: List[Dict[str, Any]] = []
    repo_count_for_org_processed_or_submitted = 0
    skipped_by_date_filter_count = 0 # Initialize counter for skipped repos

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_repo_name = {}
        try:
            # Use the materialized list if available, otherwise iterate from the org object
            iterable_repos = live_repo_list_materialized if live_repo_list_materialized is not None else organization_obj_for_iteration.get_repos(type='all')

            for repo_stub in iterable_repos:
                with processed_counter_lock:
                    if debug_limit is not None and processed_counter[0] >= debug_limit:
                        logger.info(f"Global debug limit ({debug_limit}) reached. Stopping further repository submissions for {org_name}.")
                        break
                    processed_counter[0] += 1 
                
                # --- Apply REPOS_CREATED_AFTER_DATE filter ---
                if repos_created_after_filter_date:
                    is_private_repo = repo_stub.private
                    if is_private_repo:
                        created_at_dt = repo_stub.created_at.replace(tzinfo=timezone.utc) if repo_stub.created_at else None
                        # pushed_at is a good proxy for last modified on GitHub
                        modified_at_dt = repo_stub.pushed_at.replace(tzinfo=timezone.utc) if repo_stub.pushed_at else None

                        created_match = created_at_dt and created_at_dt >= repos_created_after_filter_date
                        modified_match = modified_at_dt and modified_at_dt >= repos_created_after_filter_date

                        if created_match or modified_match:
                            # This private repo passes the filter. Log why.
                            created_at_log_str = created_at_dt.strftime('%Y-%m-%d %H:%M:%S %Z') if created_at_dt else 'N/A'
                            modified_at_log_str = modified_at_dt.strftime('%Y-%m-%d %H:%M:%S %Z') if modified_at_dt else 'N/A'
                            
                            log_message_parts = [
                                f"GitHub: Private repo '{repo_stub.full_name}' included "
                            ]
                            if created_match:
                                log_message_parts.append(f"due to Creation date ({created_at_log_str}).")
                            elif modified_match:
                                log_message_parts.append(f"due to Modification date ({modified_at_log_str}).")
                            logger.info(" ".join(log_message_parts))
                            # Repo passes, so it continues to further processing.
                        else:
                            # This private repo does NOT pass the filter.
                            # The log for skipping by this specific filter is removed as per request.
                            # Still need to skip it and adjust counter.
                            with processed_counter_lock:
                                processed_counter[0] -=1
                            skipped_by_date_filter_count += 1 # Increment skipped counter
                            continue # Skip to the next repository
                # --- End REPOS_CREATED_AFTER_DATE filter ---

                repo_count_for_org_processed_or_submitted +=1
                # --- Get current commit SHA for caching comparison ---
                current_commit_sha_for_cache = None
                repo_stub_full_name = repo_stub.full_name # For logging
                try:
                    # Optimization: don't fetch SHA for already skipped types or if no default branch
                    if not repo_stub.archived and not repo_stub.fork and repo_stub.default_branch:
                        # This is an API call to get the specific branch details
                        apply_dynamic_github_delay(cfg_obj, num_repos_in_target, max_workers) # Delay before this critical API call
                        branch_obj = repo_stub.get_branch(repo_stub.default_branch)
                        current_commit_sha_for_cache = branch_obj.commit.sha
                        logger.debug(f"Successfully fetched current commit SHA '{current_commit_sha_for_cache}' for default branch '{repo_stub.default_branch}' of {repo_stub_full_name}.")
                    elif not repo_stub.default_branch:
                        logger.warning(f"Repo {repo_stub_full_name} has no default branch. Cannot get current commit SHA for caching.")
                except GithubException as e_sha_fetch:
                    if e_sha_fetch.status == 404 and isinstance(e_sha_fetch.data, dict) and e_sha_fetch.data.get('message') == 'This repository is empty.':
                        logger.info(f"Repo {repo_stub_full_name} is empty (confirmed by get_branch). Cannot get current commit SHA for caching.")
                    elif e_sha_fetch.status == 409: # Can indicate empty or no default branch
                        logger.info(f"Repo {repo_stub_full_name} likely empty or no default branch (409 on get_branch). Cannot get current commit SHA for caching.")
                    elif isinstance(e_sha_fetch, RateLimitExceededException):
                        logger.error(f"GitHub API Rate limit exceeded while fetching current commit SHA for {repo_stub_full_name}. Will proceed without SHA, likely causing cache miss.")
                    else:
                        logger.warning(f"GitHub API error fetching current commit SHA for {repo_stub_full_name}: {e_sha_fetch.status} {getattr(e_sha_fetch, 'data', str(e_sha_fetch))}. Proceeding without SHA for caching.")
                except Exception as e_sha_unexpected:
                    logger.error(f"Unexpected error fetching current commit SHA for {repo_stub_full_name}: {e_sha_unexpected}. Proceeding without SHA for caching.", exc_info=True)

                future = executor.submit(
                    _process_single_github_repository,
                    repo_stub, 
                    org_name=org_name,
                    token=token,
                    github_instance_url=github_instance_url,
                    hours_per_commit=hours_per_commit,
                    cfg_obj=cfg_obj,  
                    inter_repo_adaptive_delay_seconds=inter_repo_adaptive_delay_per_repo,
                    num_repos_in_target=num_repos_in_target,
                    previous_scan_cache=previous_scan_cache, # Pass cache
                    current_commit_sha=current_commit_sha_for_cache, # Pass current SHA
                    num_workers=max_workers
                )
                future_to_repo_name[future] = repo_stub.full_name
        
        except RateLimitExceededException as rle_iter:
            logger.error(f"GitHub API rate limit hit during initial repository listing for {org_name}. Processing submitted tasks. Details: {rle_iter}")
        except GithubException as gh_ex_iter:
            logger.error(f"GitHub API error during initial repository listing for {org_name}: {gh_ex_iter}. Processing submitted tasks.")
        except Exception as ex_iter:
            logger.error(f"Unexpected error during initial repository listing for {org_name}: {ex_iter}. Processing submitted tasks.")

        for future in as_completed(future_to_repo_name):
            repo_name_for_log = future_to_repo_name[future]
            try:
                repo_data_result = future.result()
                if repo_data_result:
                    if repo_data_result.get("processing_status") == "skipped_fork":
                        pass 
                    else:
                        processed_repo_list.append(repo_data_result)
            except Exception as exc:
                logger.error(f"Repository {repo_name_for_log} generated an exception in its thread: {exc}", exc_info=True)
                processed_repo_list.append({"name": repo_name_for_log.split('/')[-1] if '/' in repo_name_for_log else repo_name_for_log, 
                                            "organization": org_name, 
                                            "processing_error": f"Thread execution failed: {exc}"})

    logger.info(f"Finished processing for {repo_count_for_org_processed_or_submitted} repositories from GitHub organization: {org_name}. Collected {len(processed_repo_list)} results.")
    if repos_created_after_filter_date and skipped_by_date_filter_count > 0:
        logger.info(f"GitHub: Skipped {skipped_by_date_filter_count} private repositories from '{org_name}' due to the REPOS_CREATED_AFTER_DATE filter ('{repos_created_after_filter_date.strftime('%Y-%m-%d')}').")

    return processed_repo_list

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    test_gh_token = os.getenv("GITHUB_TOKEN_TEST") 
    test_org_name_env = os.getenv("GITHUB_ORGS_TEST", "").split(',')[0].strip() 
    test_ghes_url_env = os.getenv("GITHUB_ENTERPRISE_URL_TEST")


    if not test_gh_token or is_placeholder_token(test_gh_token):
        logger.error("Test GitHub token (GITHUB_TOKEN_TEST) not found or is a placeholder in .env.")
    elif not test_org_name_env:
        logger.error("No GitHub organization found in GITHUB_ORGS_TEST in .env for testing.")
    else:
        instance_for_test = test_ghes_url_env or "public GitHub.com"
        logger.info(f"--- Testing GitHub Connector for organization: {test_org_name_env} on instance: {instance_for_test} ---")
        counter = [0]
        counter_lock = threading.Lock() 
        
        repositories = fetch_repositories(
            token=test_gh_token, 
            organization=test_org_name_env, 
            processed_counter=counter, 
            processed_counter_lock=counter_lock,
            debug_limit=None, 
            github_instance_url=test_ghes_url_env,
            previous_scan_output_file=None # No cache for direct test
        )

        if repositories:
            logger.info(f"Successfully fetched {len(repositories)} repositories.")
            for i, repo_info in enumerate(repositories[:3]): 
                logger.info(f"--- Repository {i+1} ({repo_info.get('name')}) ---")
                logger.info(f"  Repo ID: {repo_info.get('repo_id')}")
                logger.info(f"  Name: {repo_info.get('name')}")
                logger.info(f"  Org: {repo_info.get('organization')}")
                logger.info(f"  Description: {repo_info.get('description')}")
                logger.info(f"  Visibility: {repo_info.get('repositoryVisibility')}")
                logger.info(f"  Archived (temp): {repo_info.get('archived')}")
                logger.info(f"  API Tags (temp): {repo_info.get('_api_tags')}")
                logger.info(f"  Permissions: {repo_info.get('permissions')}")
                logger.info(f"  Contact: {repo_info.get('contact')}")
                if "processing_error" in repo_info:
                    logger.error(f"  Processing Error: {repo_info['processing_error']}")
            if len(repositories) > 3:
                logger.info(f"... and {len(repositories)-3} more repositories.")
        else:
            logger.warning("No repositories fetched or an error occurred.")
        logger.info(f"Total repositories processed according to counter: {counter[0]}")
