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
from datetime import timezone
from utils.labor_hrs_estimator import analyze_github_repo_sync # Import the estimator

from github import (
    Github, GithubException, UnknownObjectException, RateLimitExceededException
)

# ANSI escape codes for coloring output
ANSI_YELLOW = "\x1b[33;1m"
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


def apply_dynamic_github_delay(cfg_obj: Optional[Any], num_repos_in_target: Optional[int]):
    """
    Calculates and applies a dynamic delay based on the number of repositories in the target.
    This is a synchronous sleep for PyGithub calls.
    """
    delay_seconds = 0.0
    if cfg_obj:
        base_delay = float(getattr(cfg_obj, 'GITHUB_POST_API_CALL_DELAY_SECONDS_ENV', os.getenv("GITHUB_POST_API_CALL_DELAY_SECONDS", "0.0")))
        threshold = int(getattr(cfg_obj, 'DYNAMIC_DELAY_THRESHOLD_REPOS_ENV', os.getenv("DYNAMIC_DELAY_THRESHOLD_REPOS", "100")))
        scale = float(getattr(cfg_obj, 'DYNAMIC_DELAY_SCALE_FACTOR_ENV', os.getenv("DYNAMIC_DELAY_SCALE_FACTOR", "1.5")))
        max_d = float(getattr(cfg_obj, 'DYNAMIC_DELAY_MAX_SECONDS_ENV', os.getenv("DYNAMIC_DELAY_MAX_SECONDS", "1.0")))

        delay_seconds = calculate_dynamic_delay(
            base_delay_seconds=base_delay,
            num_items=num_repos_in_target if num_repos_in_target is not None and num_repos_in_target > 0 else None,
            threshold_items=threshold, scale_factor=scale, max_delay_seconds=max_d
        )

    if delay_seconds > 0:
        logger.debug(f"Applying SYNC dynamic GitHub API call delay: {delay_seconds:.2f}s (based on target size: {num_repos_in_target})")
        time.sleep(delay_seconds)


def is_placeholder_token(token: Optional[str]) -> bool:
    """Checks if the GitHub token is missing or a known placeholder."""
    return not token or token == PLACEHOLDER_GITHUB_TOKEN

def _get_readme_details_pygithub(repo_obj, cfg_obj: Optional[Any], num_repos_in_target: Optional[int]) -> tuple[Optional[str], Optional[str], bool]:
    """
    Fetches and decodes the README content and its HTML URL.
    Tries common README filenames.
    Returns: (content, url, is_empty_repo_error_occurred)
    """
    common_readme_names = ["README.md", "README.txt", "README", "readme.md"]
    for readme_name in common_readme_names:
        try:
            apply_dynamic_github_delay(cfg_obj, num_repos_in_target) # Apply delay BEFORE the call
            readme_file = repo_obj.get_contents(readme_name)
            readme_content_bytes = base64.b64decode(readme_file.content)
            try:
                readme_content_str = readme_content_bytes.decode('utf-8')
            except UnicodeDecodeError:
                try:
                    readme_content_str = readme_content_bytes.decode('latin-1')
                except Exception:
                    readme_content_str = readme_content_bytes.decode('utf-8', errors='ignore')
            
            readme_url = readme_file.html_url 
            logger.debug(f"Successfully fetched README '{readme_name}' (URL: {readme_url}) for {repo_obj.full_name}")
            return readme_content_str, readme_url, False
        except UnknownObjectException:
            logger.debug(f"README '{readme_name}' not found in {repo_obj.full_name}")
            continue
        except GithubException as e:
            if e.status == 404 and isinstance(e.data, dict) and e.data.get('message') == 'This repository is empty.':
                logger.info(f"Fetching README '{readme_name}' for {repo_obj.full_name} failed: GitHub API indicates repository is empty.")
                return None, None, True # Signal empty repo error
            else:
                logger.error(f"GitHub API warning fetching README '{readme_name}' for {repo_obj.full_name}: {e.status} {getattr(e, 'data', str(e))}", exc_info=False)
                return None, None, False # Stop if other API error, not an empty repo error
        except Exception as e:
            logger.error(f"Unexpected error decoding README '{readme_name}' for {repo_obj.full_name}: {e}", exc_info=True)
            return None, None, False
    logger.debug(f"No common README file found for {repo_obj.full_name}")
    return None, None, False


def _get_codeowners_content_pygithub(repo_obj, cfg_obj: Optional[Any], num_repos_in_target: Optional[int]) -> tuple[Optional[str], bool]:
    """
    Fetches CODEOWNERS content from standard locations.
    Returns: (content, is_empty_repo_error_occurred)
    """
    codeowners_locations = ["CODEOWNERS", ".github/CODEOWNERS", "docs/CODEOWNERS"]
    for location in codeowners_locations:
        try:
            apply_dynamic_github_delay(cfg_obj, num_repos_in_target) # Apply delay BEFORE the call
            codeowners_file = repo_obj.get_contents(location)
            codeowners_content = codeowners_file.decoded_content.decode('utf-8', errors='replace')
            logger.debug(f"Successfully fetched CODEOWNERS from '{location}' for {repo_obj.full_name}")
            return codeowners_content, False
        except UnknownObjectException:
            logger.debug(f"CODEOWNERS file not found at '{location}' in {repo_obj.full_name}")
            continue
        except GithubException as e:
            if e.status == 404 and isinstance(e.data, dict) and e.data.get('message') == 'This repository is empty.':
                logger.info(f"Fetching CODEOWNERS from '{location}' for {repo_obj.full_name} failed: GitHub API indicates repository is empty.")
                return None, True # Signal empty repo error
            else:
                logger.error(f"GitHub API warning fetching CODEOWNERS from '{location}' for {repo_obj.full_name}: {e.status} {getattr(e, 'data', str(e))}", exc_info=False)
                return None, False # Stop if other API error, not an empty repo error
        except Exception as e:
            logger.error(f"Unexpected error decoding CODEOWNERS from '{location}' for {repo_obj.full_name}: {e}", exc_info=True)
            return None, False
    logger.debug(f"No CODEOWNERS file found in standard locations for {repo_obj.full_name}")
    return None, False


def _fetch_tags_pygithub(repo_obj, cfg_obj: Optional[Any], num_repos_in_target: Optional[int]) -> List[str]:
    tag_names = []
    try:
        logger.debug(f"Fetching tags for repo: {repo_obj.full_name}")
        apply_dynamic_github_delay(cfg_obj, num_repos_in_target) # Apply delay BEFORE the call
        tags = repo_obj.get_tags()
        tag_names = [tag.name for tag in tags if tag.name]
        logger.debug(f"Found {len(tag_names)} tags for {repo_obj.full_name}")
    except RateLimitExceededException:
        logger.error(f"Rate limit exceeded while fetching tags for {repo_obj.full_name}. Skipping tags for this repo.")
    except GithubException as e:
        logger.error(f"GitHub API warning fetching tags for {repo_obj.full_name}: {e.status} {getattr(e, 'data', str(e))}", exc_info=False)
    except Exception as e: # Catch other potential errors like network issues during this specific call
        logger.error(f"Unexpected error fetching tags for {repo_obj.full_name}: {e}", exc_info=True)
    return tag_names

def _process_single_github_repository(
    repo, # PyGithub Repository object
    org_name: str,
    token: Optional[str], 
    github_instance_url: Optional[str],
    hours_per_commit: Optional[float],
    cfg_obj: Any, # Pass the Config object
    inter_repo_adaptive_delay_seconds: float, # Inter-repository adaptive delay
    num_repos_in_target: Optional[int] # Pass the count for dynamic delay
) -> Dict[str, Any]:
    """
    Processes a single GitHub repository to extract its metadata.
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

    Returns:
        A dictionary containing processed metadata for the repository.
    """
    repo_full_name = repo.full_name
    logger.info(f"Processing repository: {repo_full_name}")
    repo_data: Dict[str, Any] = {"name": repo.name, "organization": org_name}

    try:
        if repo.fork:
            logger.info(f"Skipping forked repository: {repo_full_name}")
            repo_data["processing_status"] = "skipped_fork"
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
            apply_dynamic_github_delay(cfg_obj, num_repos_in_target) # Apply delay BEFORE the call
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
        
        readme_content_str, readme_html_url, readme_empty_repo_error = _get_readme_details_pygithub(repo, cfg_obj, num_repos_in_target)
        codeowners_content_str, codeowners_empty_repo_error = _get_codeowners_content_pygithub(repo, cfg_obj, num_repos_in_target)

        if not repo_data.get('_is_empty_repo', False):
            repo_data['_is_empty_repo'] = readme_empty_repo_error or codeowners_empty_repo_error

        apply_dynamic_github_delay(cfg_obj, num_repos_in_target) # Delay before get_topics
        repo_topics = repo.get_topics()
        repo_git_tags = _fetch_tags_pygithub(repo, cfg_obj, num_repos_in_target)
        
        repo_data.update({
            "description": repo.description or "",
            "repositoryURL": repo.html_url,
            "homepageURL": repo.homepage or "", 
            "downloadURL": None, 
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
                "usageType": "openSource", 
                "exemptionText": None,
                "licenses": licenses_list
            },
            "contact": {}, 
            "contractNumber": None, 
            "readme_content": readme_content_str,
            "_codeowners_content": codeowners_content_str,
            "repo_id": repo.id, 
            "readme_url": readme_html_url, 
            "_api_tags": repo_git_tags, 
            "archived": repo.archived,
        })
        repo_data.setdefault('_is_empty_repo', False)
        
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
                    num_repos_in_target=num_repos_in_target 
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
    cfg_obj: Optional[Any] = None 
) -> list[dict]:
    """
    Fetches repository details from a specific GitHub organization concurrently.
    """
    instance_msg = f"GitHub instance: {github_instance_url}" if github_instance_url else "public GitHub.com"
    logger.info(f"Attempting to fetch repositories CONCURRENTLY for GitHub organization: {org_name} on {instance_msg} (max_workers: {max_workers})")

    if is_placeholder_token(token):
        logger.error("GitHub token is a placeholder or missing. Cannot fetch repositories.")
        return []

    num_repos_in_target = 0
    inter_repo_adaptive_delay_per_repo = 0.0
    organization_obj_for_iteration = None

    if cfg_obj and cfg_obj.ADAPTIVE_DELAY_ENABLED_ENV:
        try:
            logger.info(f"GitHub: Counting repositories in '{org_name}' for adaptive delay...")
            temp_gh_for_count_url = None
            if github_instance_url:
                temp_gh_for_count_url = github_instance_url.rstrip('/') + "/api/v3" if not github_instance_url.endswith("/api/v3") else github_instance_url
            
            effective_base_url_for_count = temp_gh_for_count_url if temp_gh_for_count_url else "https://api.github.com"
            temp_gh_for_count = Github(base_url=effective_base_url_for_count, login_or_token=token, timeout=30)
            
            organization_obj_for_iteration = temp_gh_for_count.get_organization(org_name)
            
            all_repo_stubs_for_count = list(organization_obj_for_iteration.get_repos(type='all'))
            num_repos_in_target = len(all_repo_stubs_for_count)
            logger.info(f"GitHub: Found {num_repos_in_target} repositories in '{org_name}'.")

            if num_repos_in_target > cfg_obj.ADAPTIVE_DELAY_THRESHOLD_REPOS_ENV:
                excess_repos = num_repos_in_target - cfg_obj.ADAPTIVE_DELAY_THRESHOLD_REPOS_ENV
                scale_factor = 1 + (excess_repos / cfg_obj.ADAPTIVE_DELAY_THRESHOLD_REPOS_ENV) 
                calculated_delay = cfg_obj.ADAPTIVE_DELAY_BASE_SECONDS_ENV * scale_factor
                inter_repo_adaptive_delay_per_repo = min(calculated_delay, cfg_obj.ADAPTIVE_DELAY_MAX_SECONDS_ENV)
            if inter_repo_adaptive_delay_per_repo > 0:
                logger.info(f"{ANSI_YELLOW}GitHub: INTER-REPO adaptive delay calculated for '{org_name}': {inter_repo_adaptive_delay_per_repo:.2f}s per repo (based on {num_repos_in_target} repos).{ANSI_RESET}")
            elif num_repos_in_target > 0 :
                logger.info(f"GitHub: Adaptive delay not applied for '{org_name}' (num_repos: {num_repos_in_target}, threshold: {cfg_obj.ADAPTIVE_DELAY_THRESHOLD_REPOS_ENV}).")
        except Exception as count_err:
            logger.warning(f"GitHub: Error counting repositories in '{org_name}' for adaptive delay (details in platform log). Proceeding without adaptive delay for this target.")
            platform_logger_name = f"clients.github.{org_name.replace('.', '_')}"
            platform_specific_logger = logging.getLogger(platform_logger_name)
            if platform_specific_logger.hasHandlers():
                platform_specific_logger.error(f"Detailed error counting repositories in '{org_name}' for adaptive delay:", exc_info=True)
    elif cfg_obj:
        logger.info(f"GitHub: Adaptive delay is disabled by configuration for '{org_name}'.")

    try:
        gh_url_for_org_processing = None
        if github_instance_url:
            gh_url_for_org_processing = github_instance_url.rstrip('/') + "/api/v3" if not github_instance_url.endswith("/api/v3") else github_instance_url
        
        # Ensure base_url is always a string, defaulting to public GitHub API if gh_url_for_org_processing is None
        effective_base_url_for_gh = gh_url_for_org_processing if gh_url_for_org_processing else "https://api.github.com"
        gh = Github(login_or_token=token, base_url=effective_base_url_for_gh, timeout=30)
        if not organization_obj_for_iteration:
            apply_dynamic_github_delay(cfg_obj, num_repos_in_target) # Delay before get_organization
            organization_obj_for_iteration = gh.get_organization(org_name)
        logger.info(f"Successfully configured GitHub client for organization: {org_name}.")
    except Exception as e:
        logger.critical(f"Failed to initialize GitHub client for org '{org_name}': {e}", exc_info=True)
        return []

    processed_repo_list: List[Dict[str, Any]] = []
    repo_count_for_org_processed_or_submitted = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_repo_name = {}
        try:
            apply_dynamic_github_delay(cfg_obj, num_repos_in_target) # Delay before get_repos
            iterable_repos = organization_obj_for_iteration.get_repos(type='all')

            for repo_stub in iterable_repos:
                with processed_counter_lock:
                    if debug_limit is not None and processed_counter[0] >= debug_limit:
                        logger.info(f"Global debug limit ({debug_limit}) reached. Stopping further repository submissions for {org_name}.")
                        break
                    processed_counter[0] += 1 
                
                repo_count_for_org_processed_or_submitted +=1
                future = executor.submit(
                    _process_single_github_repository,
                    repo_stub, 
                    org_name,
                    token,
                    github_instance_url,
                    hours_per_commit,
                    cfg_obj, 
                    inter_repo_adaptive_delay_per_repo,
                    num_repos_in_target 
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
            org_name=test_org_name_env, 
            processed_counter=counter, 
            processed_counter_lock=counter_lock,
            debug_limit=None, 
            github_instance_url=test_ghes_url_env
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
