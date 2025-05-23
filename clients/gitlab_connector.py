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


import gitlab # python-gitlab library
from gitlab.exceptions import GitlabAuthenticationError, GitlabGetError, GitlabListError

# ANSI escape codes for coloring output
ANSI_RED = "\x1b[31;1m"  # Bold Red
ANSI_YELLOW = "\x1b[333;1m"
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
        def process_repository_exemptions(self, repo_data: Dict[str, Any], default_org_identifiers: Optional[List[str]] = None) -> Dict[str, Any]:
            # This mock modifies repo_data in place and returns it,
            # ensuring it has expected keys if exemption_processor would add them.
            repo_data.setdefault('_status_from_readme', None)
            repo_data.setdefault('_private_contact_emails', [])
            repo_data.setdefault('contact', {})
            repo_data.setdefault('permissions', {"usageType": "openSource"}) # Default
            # Mock processor also removes these if the real one does
            repo_data.pop('readme_content', None)
            repo_data.pop('_codeowners_content', None)
            repo_data.pop('is_empty_repo', None)
            return repo_data
    exemption_processor = MockExemptionProcessor()

# load_dotenv() # No longer loading .env directly for auth in this connector
logger = logging.getLogger(__name__)

PLACEHOLDER_GITLAB_TOKEN = "YOUR_GITLAB_PAT" # Common placeholder for GitLab PAT

def is_placeholder_token(token: Optional[str]) -> bool:
    """Checks if the GitLab token is missing or a known placeholder."""
    return not token or token == PLACEHOLDER_GITLAB_TOKEN


def _get_readme_content_gitlab(project_obj, cfg_obj: Optional[Any], dynamic_delay_to_apply: float, num_workers: int = 1) -> tuple[Optional[str], Optional[str]]:
    """
    Fetches and decodes the README content for a given GitLab project object.
    Tries common README filenames. Returns content and URL.
    """
    common_readme_names = ["README.md", "README.txt", "README", "readme.md"]
    if not project_obj.default_branch:
        logger.warning(f"Cannot fetch README for {project_obj.path_with_namespace}: No default branch set.")
        return None, None
    for readme_name in common_readme_names:
        try:
            readme_file = project_obj.files.get(file_path=readme_name, ref=project_obj.default_branch)
            readme_content_bytes = base64.b64decode(readme_file.content)
            readme_content_str = readme_content_bytes.decode('utf-8', errors='replace')
            readme_url = f"{project_obj.web_url}/-/blob/{project_obj.default_branch}/{readme_name.lstrip('/')}"
            logger.debug(f"Successfully fetched README '{readme_name}' for {project_obj.path_with_namespace}")
            if dynamic_delay_to_apply > 0:
                logger.debug(f"GitLab applying SYNC post-API call delay (get README file): {dynamic_delay_to_apply:.2f}s")
                time.sleep(dynamic_delay_to_apply)
            return readme_content_str, readme_url
        except GitlabGetError as e:
            if e.response_code == 404:
                logger.debug(f"README '{readme_name}' not found in {project_obj.path_with_namespace}")
                continue
            else:
                logger.error(f"GitLab API error fetching README '{readme_name}' for {project_obj.path_with_namespace}: {e}", exc_info=False)
                return None, None 
        except Exception as e:
            logger.error(f"Unexpected error decoding README '{readme_name}' for {project_obj.path_with_namespace}: {e}", exc_info=True)
            return None, None
    logger.debug(f"No common README file found for {project_obj.path_with_namespace}")
    return None, None


def _get_codeowners_content_gitlab(project_obj, cfg_obj: Optional[Any], dynamic_delay_to_apply: float, num_workers: int = 1) -> Optional[str]:
    """Fetches CODEOWNERS content from standard locations in a GitLab project."""
    common_paths = ["CODEOWNERS", ".gitlab/CODEOWNERS", "docs/CODEOWNERS"]
    if not project_obj.default_branch:
        logger.warning(f"Cannot fetch CODEOWNERS for {project_obj.path_with_namespace}: No default branch set.")
        return None
    for path in common_paths:
        try:
            content_file = project_obj.files.get(file_path=path.lstrip('/'), ref=project_obj.default_branch)
            content_bytes = base64.b64decode(content_file.content)
            content_str = content_bytes.decode('utf-8', errors='replace')
            logger.debug(f"Successfully fetched CODEOWNERS from '{path}' for {project_obj.path_with_namespace}")
            if dynamic_delay_to_apply > 0:
                logger.debug(f"GitLab applying SYNC post-API call delay (get CODEOWNERS file): {dynamic_delay_to_apply:.2f}s")
                time.sleep(dynamic_delay_to_apply)
            return content_str
        except GitlabGetError as e:
            if e.response_code == 404:
                continue
            else:
                logger.error(f"GitLab API error fetching CODEOWNERS at {path} for {project_obj.path_with_namespace}: {e}", exc_info=False)
                return None
        except Exception as e:
            logger.error(f"Unexpected error fetching CODEOWNERS at {path} for {project_obj.path_with_namespace}: {e}", exc_info=True)
            return None
    logger.debug(f"No CODEOWNERS file found in standard locations for {project_obj.path_with_namespace}")
    return None


def _fetch_tags_gitlab(project_obj, cfg_obj: Optional[Any], dynamic_delay_to_apply: float, num_workers: int = 1) -> List[str]:
    """Fetches Git tag names using the python-gitlab project object."""
    tag_names = []
    try:
        logger.debug(f"Fetching Git tags for project: {project_obj.path_with_namespace}")
        tags = project_obj.tags.list(all=True) 
        if dynamic_delay_to_apply > 0:
            logger.debug(f"GitLab applying SYNC post-API call delay (list tags): {dynamic_delay_to_apply:.2f}s")
            time.sleep(dynamic_delay_to_apply)
        tag_names = [tag.name for tag in tags if tag.name]
        logger.debug(f"Found {len(tag_names)} Git tags for {project_obj.path_with_namespace}")
    except GitlabListError as e:
         logger.error(f"GitLab API error listing Git tags for {project_obj.path_with_namespace}: {e}", exc_info=False)
    except Exception as e:
        logger.error(f"Unexpected error fetching Git tags for {project_obj.path_with_namespace}: {e}", exc_info=True)
    return tag_names

def _process_single_gitlab_project(
    gl_instance: gitlab.Gitlab,
    project_stub_id: int,
    group_full_path: str,
    token: Optional[str],
    effective_gitlab_url: str,
    hours_per_commit: Optional[float],
    cfg_obj: Any,
    inter_repo_adaptive_delay_seconds: float,
    dynamic_post_api_call_delay_seconds: float,
    previous_scan_cache: Dict[str, Dict],
    current_commit_sha: Optional[str],
    num_workers: int = 1  # Add this parameter
) -> Dict[str, Any]:
    """
    Processes a single GitLab project to extract its metadata.
    This function is intended to be run in a separate thread.
    """
    repo_data: Dict[str, Any] = {}
    project: Optional[gitlab.objects.Project] = None # Type hint for clarity
    gitlab_cache_config = PLATFORM_CACHE_CONFIG["gitlab"]

    try:
        # Get full project object
        project = gl_instance.projects.get(project_stub_id, lazy=False, statistics=True)
        project_id_str = str(project.id) # Key for caching

        # --- Caching Logic ---
        if current_commit_sha: # Only attempt cache hit if we have a current SHA to compare
            cached_repo_entry = previous_scan_cache.get(project_id_str)
            if cached_repo_entry:
                cached_commit_sha = cached_repo_entry.get(gitlab_cache_config["commit_sha_field"])
                if cached_commit_sha and current_commit_sha == cached_commit_sha:
                    logger.info(f"CACHE HIT: GitLab project '{project.path_with_namespace}' (ID: {project_id_str}) has not changed. Using cached data.")
                    
                    # Start with the cached data
                    repo_data_to_process = cached_repo_entry.copy()
                    # Ensure the current (and matching) SHA is in the data for consistency
                    repo_data_to_process[gitlab_cache_config["commit_sha_field"]] = current_commit_sha

                    # Ensure 'repo_id' is present, mapping from 'id' if necessary for older cached formats
                    if "repo_id" not in repo_data_to_process and "id" in repo_data_to_process:
                        logger.debug(f"CACHE HIT {project.path_with_namespace}: Mapping 'id' ({repo_data_to_process['id']}) to 'repo_id' from cached data.")
                        repo_data_to_process["repo_id"] = repo_data_to_process["id"]
                        repo_data_to_process.pop("id", None) 
                    
                    # Re-process exemptions to apply current logic/AI models, even on cached data
                    if cfg_obj:
                        repo_data_to_process = exemption_processor.process_repository_exemptions(
                            repo_data_to_process, default_org_identifiers=[group_full_path],
                            ai_is_enabled_from_config=cfg_obj.AI_ENABLED_ENV, ai_model_name_from_config=cfg_obj.AI_MODEL_NAME_ENV,
                            ai_temperature_from_config=cfg_obj.AI_TEMPERATURE_ENV, ai_max_output_tokens_from_config=cfg_obj.AI_MAX_OUTPUT_TOKENS_ENV,
                            ai_max_input_tokens_from_config=cfg_obj.MAX_TOKENS_ENV)
                    return repo_data_to_process # Return cached and re-processed data
        
        logger.info(f"No SHA: Processing GitLab project: {project.path_with_namespace} (ID: {project_id_str}) with full data fetch.")


        if dynamic_post_api_call_delay_seconds > 0:
            logger.debug(f"GitLab applying SYNC post-API call delay (get project details): {dynamic_post_api_call_delay_seconds:.2f}s")
            time.sleep(dynamic_post_api_call_delay_seconds)

        repo_full_name = project.path_with_namespace
        repo_data["name"] = project.path
        repo_data["organization"] = group_full_path # Use the parent group path

        # logger.info(f"Processing repository: {repo_full_name}") # Already logged above with cache status
        
        if hasattr(project, 'forked_from_project') and project.forked_from_project:
            logger.info(f"Skipping forked repository: {repo_full_name}")
            repo_data["processing_status"] = "skipped_fork"
            return repo_data

        repo_data['_is_empty_repo'] = False
        if project.empty_repo:
            logger.info(f"Repository {repo_full_name} is marked as empty by GitLab API (project.empty_repo is True).")
            repo_data['_is_empty_repo'] = True
        elif hasattr(project, 'statistics') and project.statistics and project.statistics.get('commit_count', -1) == 0:
            logger.info(f"Repository {repo_full_name} has 0 commits according to statistics, treating as effectively empty for content processing.")
            repo_data['_is_empty_repo'] = True
        
        repo_description = project.description if project.description else ""
        
        visibility_status = project.visibility
        if visibility_status not in ["public", "private", "internal"]:
            logger.warning(f"Unknown visibility '{visibility_status}' for {repo_full_name}. Defaulting to 'private'.")
            visibility_status = "private"

        created_at_dt: Optional[datetime] = None
        if project.created_at:
            try:
                created_at_dt = datetime.fromisoformat(project.created_at.replace('Z', '+00:00')).replace(tzinfo=timezone.utc)
            except ValueError:
                logger.warning(f"Could not parse created_at date string '{project.created_at}' for {repo_full_name}")

        last_activity_at_dt: Optional[datetime] = None
        if project.last_activity_at:
            try:
                last_activity_at_dt = datetime.fromisoformat(project.last_activity_at.replace('Z', '+00:00')).replace(tzinfo=timezone.utc)
            except ValueError:
                logger.warning(f"Could not parse last_activity_at date string '{project.last_activity_at}' for {repo_full_name}")
        
        all_languages_list = []
        try:
            languages_dict = project.languages()
            if languages_dict:
                all_languages_list = list(languages_dict.keys())
            if dynamic_post_api_call_delay_seconds > 0:
                logger.debug(f"GitLab applying SYNC post-API call delay (get languages): {dynamic_post_api_call_delay_seconds:.2f}s")
                time.sleep(dynamic_post_api_call_delay_seconds)
        except Exception as lang_err:
            logger.warning(f"Could not fetch languages for {repo_full_name}: {lang_err}", exc_info=False)

        readme_content, readme_html_url = _get_readme_content_gitlab(project, cfg_obj, dynamic_post_api_call_delay_seconds, num_workers)
        codeowners_content = _get_codeowners_content_gitlab(project, cfg_obj, dynamic_post_api_call_delay_seconds, num_workers)
        repo_topics = project.tag_list if hasattr(project, 'tag_list') else []
        repo_git_tags = _fetch_tags_gitlab(project, cfg_obj, dynamic_post_api_call_delay_seconds, num_workers)

        licenses_list = []
        if hasattr(project, 'license') and project.license and isinstance(project.license, dict):
            license_entry = {"spdxID": project.license.get('key'), "name": project.license.get('name'), "URL": project.license.get('html_url')}
            licenses_list.append({k: v for k, v in license_entry.items() if v})

        repo_data.update({
            "description": repo_description, 
            "repositoryURL": project.web_url, 
            "homepageURL": project.web_url,
            "downloadURL": None, 
            "readme_url": readme_html_url, 
            "vcs": "git", 
            "repositoryVisibility": visibility_status,
            "status": "development", 
            "version": "N/A", 
            "laborHours": 0, 
            "languages": all_languages_list,
            "tags": repo_topics,
            "date": {"created": created_at_dt.isoformat() if created_at_dt else None, "lastModified": last_activity_at_dt.isoformat() if last_activity_at_dt else None},
            "permissions": {"usageType": "openSource", 
                            "exemptionText": None, 
                            "licenses": licenses_list},
            "contact": {}, 
            "contractNumber": None, 
            "readme_content": readme_content,
            "_codeowners_content": codeowners_content,
            "repo_id": project.id, 
            "_api_tags": repo_git_tags, 
            "archived": project.archived
        })
        repo_data.setdefault('_is_empty_repo', False)
        # Store the current commit SHA for the next scan's cache, if available
        if current_commit_sha:
            repo_data[gitlab_cache_config["commit_sha_field"]] = current_commit_sha


        if hours_per_commit is not None:
            logger.debug(f"Estimating labor hours for GitLab repo: {project.path_with_namespace}")
            labor_df = analyze_gitlab_repo_sync(
                project_id=str(project.id), token=token, 
                hours_per_commit=hours_per_commit, 
                gitlab_api_url=effective_gitlab_url,
                cfg_obj=cfg_obj, # Pass cfg_obj for its own post-API call delays
                num_repos_in_target=None, # Labor estimator doesn't need this for its *own* calls, it gets it from cfg_obj
                is_empty_repo=repo_data.get('_is_empty_repo', False)
            )
            repo_data["laborHours"] = round(float(labor_df["EstimatedHours"].sum()), 2) if not labor_df.empty else 0.0
            if repo_data["laborHours"] > 0: logger.info(f"Estimated labor hours for {project.path_with_namespace}: {repo_data['laborHours']}")
        
        if cfg_obj:
            repo_data = exemption_processor.process_repository_exemptions(
                repo_data,
                default_org_identifiers=[group_full_path],
                ai_is_enabled_from_config=cfg_obj.AI_ENABLED_ENV,
                ai_model_name_from_config=cfg_obj.AI_MODEL_NAME_ENV,
                ai_temperature_from_config=cfg_obj.AI_TEMPERATURE_ENV,
                ai_max_output_tokens_from_config=cfg_obj.AI_MAX_OUTPUT_TOKENS_ENV,
                ai_max_input_tokens_from_config=cfg_obj.MAX_TOKENS_ENV
            )
            logger.info(f"GitLab Connector: For {project.path_with_namespace}, after exemption_processor (AI branch), _private_contact_emails: {repo_data.get('_private_contact_emails')}")
        else:
            logger.warning(
                f"cfg_obj not provided to _process_single_gitlab_project for {repo_full_name}. "
                "Exemption processor will use its default AI parameter values."
            )
            repo_data = exemption_processor.process_repository_exemptions(
                repo_data, default_org_identifiers=[group_full_path]
            )
            logger.info(f"GitLab Connector: For {project.path_with_namespace}, after exemption_processor (non-AI branch), _private_contact_emails: {repo_data.get('_private_contact_emails')}")
        if inter_repo_adaptive_delay_seconds > 0: # This is the inter-repository adaptive delay
            logger.debug(f"GitLab project {repo_full_name}: Applying INTER-REPO adaptive delay of {inter_repo_adaptive_delay_seconds:.2f}s")
            time.sleep(inter_repo_adaptive_delay_seconds)

        return repo_data

    except GitlabGetError as p_get_err:
        project_path_for_error = project.path if project else f"ID_{project_stub_id}"
        logger.error(f"GitLab API error getting full details for project {project_path_for_error} (ID: {project_stub_id}, part of {group_full_path}): {p_get_err}. Skipping.", exc_info=False)
        return {"name": project_path_for_error, "organization": group_full_path, "processing_error": f"GitLab API Error getting details: {p_get_err.error_message}"}
    except Exception as e_proj:
        project_path_for_error = project.path if project else f"ID_{project_stub_id}"
        logger.error(f"Unexpected error processing project {project_path_for_error} (ID: {project_stub_id}, part of {group_full_path}): {e_proj}. Skipping.", exc_info=True)
        return {"name": project_path_for_error, "organization": group_full_path, "processing_error": f"Unexpected Error: {e_proj}"}

def _parse_gitlab_iso_datetime_for_filter(datetime_str: Optional[str], logger_instance: logging.Logger, repo_name_for_log: str, field_name: str) -> Optional[datetime]:
    """Parses GitLab's ISO datetime string to a timezone-aware datetime object for filtering."""
    if not datetime_str:
        return None
    try:
        # GitLab dates are typically like '2023-10-26T18:06:07.176Z'
        # fromisoformat handles 'Z' correctly in Python 3.11+, for older versions, manual replacement is safer.
        dt_obj = datetime.fromisoformat(datetime_str.replace('Z', '+00:00'))
        return dt_obj.replace(tzinfo=timezone.utc) if dt_obj.tzinfo is None else dt_obj
    except ValueError:
        logger_instance.warning(f"Could not parse {field_name} date string '{datetime_str}' for {repo_name_for_log} during filter check.")
        return None

def fetch_repositories(
    token: Optional[str], 
    group_path: str, 
    processed_counter: List[int], 
    processed_counter_lock: threading.Lock, # Added lock
    debug_limit: int | None = None, 
    gitlab_instance_url: str | None = None,
    hours_per_commit: Optional[float] = None,
    max_workers: int = 5, 
    cfg_obj: Optional[Any] = None, # Accept the cfg object
    previous_scan_output_file: Optional[str] = None # For caching
) -> list[dict]:
    """
    Fetches repository (project) details from a specific GitLab group.

    Args:
        token: The GitLab Personal Access Token.
        group_path: The full path of the GitLab group (e.g., 'my-org/my-subgroup').
        processed_counter: Mutable list to track processed repositories for debug limit.
        debug_limit: Optional global limit for repositories to process.
        processed_counter_lock: Lock for safely updating processed_counter.
        gitlab_instance_url: The base URL of the GitLab instance. Defaults to https://gitlab.com if None.


    Returns:
        A list of dictionaries, each containing processed metadata for a repository.
    """
    # Use a default GitLab URL if none is provided or if it's an empty string
    effective_gitlab_url = gitlab_instance_url
    if not effective_gitlab_url: # Handles both None and empty string
        effective_gitlab_url = "https://gitlab.com"
        logger.warning(f"No GitLab instance URL provided or it was empty. Using default: {effective_gitlab_url}")
    
    logger.info(f"Attempting to fetch repositories CONCURRENTLY for GitLab group: {group_path} on {effective_gitlab_url} (max_workers: {max_workers})")

    if is_placeholder_token(token): # is_placeholder_token now takes token as arg
        logger.error("GitLab token is a placeholder or missing. Cannot fetch repositories.")
        return []
    if not group_path:
        logger.warning("GitLab group path not provided. Skipping GitLab scan.")
        return []
    
    # Parse the REPOS_CREATED_AFTER_DATE from cfg_obj
    repos_created_after_filter_date: Optional[datetime] = None
    if cfg_obj and hasattr(cfg_obj, 'REPOS_CREATED_AFTER_DATE'):
        repos_created_after_filter_date = parse_repos_created_after_date(cfg_obj.REPOS_CREATED_AFTER_DATE, logger)

    ssl_verify_flag = True # Default to True (verify SSL)
    disable_ssl_env = os.getenv("DISABLE_SSL_VERIFICATION", "false").lower()
    if disable_ssl_env == "true":
        ssl_verify_flag = False
        logger.warning(f"{ANSI_RED}SECURITY WARNING: SSL certificate verification is DISABLED for GitLab connections due to DISABLE_SSL_VERIFICATION=true.{ANSI_RESET}")
        logger.warning(f"{ANSI_YELLOW}This should ONLY be used for trusted internal environments. Do NOT use in production with public-facing services.{ANSI_RESET}")


    # --- Load Previous Scan Data for Caching ---
    previous_scan_cache: Dict[str, Dict] = {}
    if previous_scan_output_file:
        logger.info(f"Attempting to load previous GitLab scan data for group '{group_path}' from: {previous_scan_output_file}")
        previous_scan_cache = load_previous_scan_data(previous_scan_output_file, "gitlab")
    else:
        logger.info(f"No previous scan output file provided for GitLab group '{group_path}'. Full scan for all projects in this group.")

    processed_repo_list: List[Dict[str, Any]] = []
    gl_instance = None # To hold the authenticated gitlab instance

    try:
        gl_instance = gitlab.Gitlab(effective_gitlab_url.strip('/'), private_token=token, timeout=30, ssl_verify=ssl_verify_flag)
        gl_instance.auth() 
        logger.info(f"Successfully connected and authenticated to GitLab instance: {effective_gitlab_url}")

        group = gl_instance.groups.get(group_path, lazy=False)
        logger.info(f"Successfully found GitLab group: {group.full_path} (ID: {group.id})")

        # --- Determine num_projects_in_target for adaptive delay and dynamic intra-repo delays ---
        num_projects_in_target_for_delay_calc = 0
        inter_repo_adaptive_delay_per_repo = 0.0 # For the delay *between* processing repos
        live_project_stubs_materialized = None # To store the live list if fetched for count

        cached_project_count_for_target = 0
        if previous_scan_cache: # Check if cache was loaded and is not empty
            gitlab_id_field = PLATFORM_CACHE_CONFIG.get("gitlab", {}).get("id_field", "repo_id")
            valid_cached_projects = [
                proj_data for proj_id, proj_data in previous_scan_cache.items()
                if isinstance(proj_data, dict) and proj_data.get(gitlab_id_field) is not None
            ]
            cached_project_count_for_target = len(valid_cached_projects)
            if cached_project_count_for_target > 0:
                logger.info(f"CACHE: Found {cached_project_count_for_target} valid projects in cache for group '{group_path}'.")
                num_projects_in_target_for_delay_calc = cached_project_count_for_target
                logger.info(f"ADAPTIVE DELAY/PROCESSING: Using cached count ({num_projects_in_target_for_delay_calc}) as total items estimate for target group '{group_path}'.")

        if num_projects_in_target_for_delay_calc == 0: # If cache was empty or not used for count
            try:
                logger.info(f"ADAPTIVE DELAY/PROCESSING: Cache empty or not used for count. Fetching live project list for group '{group_path}' to get count.")
                # This is an API call
                all_live_project_stubs = list(group.projects.list(all=True, include_subgroups=True, statistics=False, lazy=True))
                initial_live_count = len(all_live_project_stubs)
                logger.info(f"ADAPTIVE DELAY/PROCESSING: Fetched {initial_live_count} live projects for group '{group_path}' before date filtering.")

                # --- Apply REPOS_CREATED_AFTER_DATE filter to live_project_stubs_materialized ---
                if repos_created_after_filter_date and all_live_project_stubs:
                    filtered_live_projects = []
                    skipped_legacy_count = 0
                    for proj_stub_item in all_live_project_stubs:
                        visibility = proj_stub_item.visibility
                        if visibility == "public": # Public projects always pass
                            filtered_live_projects.append(proj_stub_item)
                            continue
                        
                        # Non-public project ('internal', 'private'), check dates
                        created_at_dt = _parse_gitlab_iso_datetime_for_filter(proj_stub_item.created_at, logger, proj_stub_item.path_with_namespace, "created_at")
                        modified_at_dt = _parse_gitlab_iso_datetime_for_filter(proj_stub_item.last_activity_at, logger, proj_stub_item.path_with_namespace, "last_activity_at")

                        if (created_at_dt and created_at_dt >= repos_created_after_filter_date) or \
                           (modified_at_dt and modified_at_dt >= repos_created_after_filter_date):
                            filtered_live_projects.append(proj_stub_item)
                        else:
                            skipped_legacy_count += 1
                    live_project_stubs_materialized = filtered_live_projects # Update with filtered list
                    if skipped_legacy_count > 0:
                        logger.info(f"GitLab: Skipped {skipped_legacy_count} non-public legacy projects from group '{group_path}' due to REPOS_CREATED_AFTER_DATE filter ('{repos_created_after_filter_date.strftime('%Y-%m-%d')}') before full processing.")
                else:
                    live_project_stubs_materialized = all_live_project_stubs # Use all if no filter or no initial projects

                num_projects_in_target_for_delay_calc = len(live_project_stubs_materialized) # Count after filtering
                logger.info(f"ADAPTIVE DELAY/PROCESSING: Using API count of {num_projects_in_target_for_delay_calc} (after date filter) as total items estimate for target group '{group_path}'.")
            except Exception as e_live_count:
                logger.warning(f"GitLab: Error fetching live project list for group '{group_path}' to get count: {e_live_count}. num_projects_in_target_for_delay_calc will be 0.", exc_info=True)
                num_projects_in_target_for_delay_calc = 0 # Fallback

        # --- Calculate inter-repo adaptive delay if enabled ---
        if cfg_obj and cfg_obj.ADAPTIVE_DELAY_ENABLED_ENV and num_projects_in_target_for_delay_calc > 0:
            if num_projects_in_target_for_delay_calc > cfg_obj.ADAPTIVE_DELAY_THRESHOLD_REPOS_ENV:
                excess_repos = num_projects_in_target_for_delay_calc - cfg_obj.ADAPTIVE_DELAY_THRESHOLD_REPOS_ENV
                scale_factor = 1 + (excess_repos / cfg_obj.ADAPTIVE_DELAY_THRESHOLD_REPOS_ENV)
                calculated_delay = cfg_obj.ADAPTIVE_DELAY_BASE_SECONDS_ENV * scale_factor
                inter_repo_adaptive_delay_per_repo = min(calculated_delay, cfg_obj.ADAPTIVE_DELAY_MAX_SECONDS_ENV)
                if inter_repo_adaptive_delay_per_repo > 0:
                    logger.info(f"{ANSI_YELLOW}GitLab: INTER-REPO adaptive delay calculated for group '{group_path}': {inter_repo_adaptive_delay_per_repo:.2f}s per project (based on {num_projects_in_target_for_delay_calc} projects, {max_workers} workers).{ANSI_RESET}")
        elif cfg_obj and cfg_obj.ADAPTIVE_DELAY_ENABLED_ENV and num_projects_in_target_for_delay_calc == 0:
            logger.info(f"GitLab: Adaptive delay enabled but num_projects_in_target_for_delay_calc is 0 for group '{group_path}'. No inter-repo adaptive delay will be applied.")
        elif cfg_obj: # Adaptive delay is configured but disabled
            logger.info(f"GitLab: Adaptive delay is disabled by configuration for group '{group_path}'.")

        # Calculate dynamic POST-API-CALL delay for metadata calls within this target
        dynamic_post_api_call_delay_seconds = 0.0
        if cfg_obj:
            base_delay = float(getattr(cfg_obj, 'GITLAB_POST_API_CALL_DELAY_SECONDS_ENV', os.getenv("GITLAB_POST_API_CALL_DELAY_SECONDS", "0.0")))
            threshold = int(getattr(cfg_obj, 'DYNAMIC_DELAY_THRESHOLD_REPOS_ENV', os.getenv("DYNAMIC_DELAY_THRESHOLD_REPOS", "100")))
            scale = float(getattr(cfg_obj, 'DYNAMIC_DELAY_SCALE_FACTOR_ENV', os.getenv("DYNAMIC_DELAY_SCALE_FACTOR", "1.5")))
            max_d = float(getattr(cfg_obj, 'DYNAMIC_DELAY_MAX_SECONDS_ENV', os.getenv("DYNAMIC_DELAY_MAX_SECONDS", "1.0")))
            
            dynamic_post_api_call_delay_seconds = calculate_dynamic_delay(
                base_delay_seconds=base_delay,
                num_items=num_projects_in_target_for_delay_calc if num_projects_in_target_for_delay_calc > 0 else None,
                threshold_items=threshold, 
                scale_factor=scale, 
                max_delay_seconds=max_d,
                num_workers=max_workers  # Pass the number of workers
            )
            if dynamic_post_api_call_delay_seconds > 0:
                 logger.info(f"{ANSI_YELLOW}GitLab: DYNAMIC POST-API-CALL delay for metadata in group '{group_path}' set to: {dynamic_post_api_call_delay_seconds:.2f}s (based on {num_projects_in_target_for_delay_calc} projects, {max_workers} workers).{ANSI_RESET}")

        project_count_for_group_submitted = 0
        skipped_by_date_filter_count = 0 # Initialize counter for skipped projects

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_project_name = {}
            try:
                # Use the materialized list if available (from live count), otherwise get fresh iterator
                projects_iterator = live_project_stubs_materialized if live_project_stubs_materialized is not None \
                                    else group.projects.list(all=True, include_subgroups=True, statistics=False, lazy=True)
                for proj_stub in projects_iterator:
                    with processed_counter_lock:
                        if debug_limit is not None and processed_counter[0] >= debug_limit:
                            logger.info(f"Global debug limit ({debug_limit}) reached. Stopping further project submissions for {group_path}.")
                            break
                        processed_counter[0] += 1
                    
                    project_stub_path_with_namespace = proj_stub.path_with_namespace # For logging

                    # --- Apply REPOS_CREATED_AFTER_DATE filter ---
                    if repos_created_after_filter_date:
                        visibility = proj_stub.visibility # 'public', 'internal', 'private'
                        is_not_public = visibility != "public" # Treat 'internal' and 'private' as non-public

                        if is_not_public:
                            created_at_dt = _parse_gitlab_iso_datetime_for_filter(proj_stub.created_at, logger, project_stub_path_with_namespace, "created_at")
                            modified_at_dt = _parse_gitlab_iso_datetime_for_filter(proj_stub.last_activity_at, logger, project_stub_path_with_namespace, "last_activity_at")

                            created_match = created_at_dt and created_at_dt >= repos_created_after_filter_date
                            modified_match = modified_at_dt and modified_at_dt >= repos_created_after_filter_date

                            if created_match or modified_match:
                                created_at_log_str = created_at_dt.strftime('%Y-%m-%d %H:%M:%S %Z') if created_at_dt else 'N/A'
                                modified_at_log_str = modified_at_dt.strftime('%Y-%m-%d %H:%M:%S %Z') if modified_at_dt else 'N/A'
                                log_message_parts = [
                                    f"GitLab: Private repo '{project_stub_path_with_namespace}' included "
                                ]

                                if created_match:
                                    log_message_parts.append(f"due to Creation date ({created_at_log_str}).")
                                elif modified_match:
                                    log_message_parts.append(f"due to Modification date ({modified_at_log_str}).")
                                logger.info(" ".join(log_message_parts))
                            else:
                                # Skip this non-public project
                                with processed_counter_lock:
                                    processed_counter[0] -= 1
                                skipped_by_date_filter_count += 1
                                continue # Skip to the next project
                    # --- End REPOS_CREATED_AFTER_DATE filter ---

                    
                    # --- Get current commit SHA for caching comparison ---
                    current_commit_sha_for_cache = None
                    project_stub_path_with_namespace = proj_stub.path_with_namespace # For logging
                    try:
                        # Need to get the full project object to access default_branch and commits
                        # This is an API call.
                        if dynamic_post_api_call_delay_seconds > 0: # Delay before this critical API call
                            logger.debug(f"GitLab applying SYNC post-API call delay (get project for SHA): {dynamic_post_api_call_delay_seconds:.2f}s")
                            time.sleep(dynamic_post_api_call_delay_seconds)
                        
                        project_for_sha = gl_instance.projects.get(proj_stub.id, lazy=False) # Get full object
                        if project_for_sha.empty_repo:
                            logger.info(f"Project {project_stub_path_with_namespace} is empty. Cannot get current commit SHA for caching.")
                        elif project_for_sha.default_branch:
                            # Another API call to get commits
                            if dynamic_post_api_call_delay_seconds > 0: # Delay before this critical API call
                                logger.debug(f"GitLab applying SYNC post-API call delay (get commits for SHA): {dynamic_post_api_call_delay_seconds:.2f}s")
                                time.sleep(dynamic_post_api_call_delay_seconds)
                            commits = project_for_sha.commits.list(ref_name=project_for_sha.default_branch, per_page=1, get_all=False)
                            if commits:
                                current_commit_sha_for_cache = commits[0].id
                                logger.debug(f"Successfully fetched current commit SHA '{current_commit_sha_for_cache}' for default branch '{project_for_sha.default_branch}' of {project_stub_path_with_namespace}.")
                    except GitlabGetError as e_sha_fetch: # Covers project get or commit list
                        logger.warning(f"GitLab API error fetching current commit SHA for {project_stub_path_with_namespace}: {e_sha_fetch}. Proceeding without SHA for caching.")
                    except Exception as e_sha_unexpected:
                        logger.error(f"Unexpected error fetching current commit SHA for {project_stub_path_with_namespace}: {e_sha_unexpected}. Proceeding without SHA for caching.", exc_info=True)

                    project_count_for_group_submitted += 1
                    future = executor.submit(
                        _process_single_gitlab_project,
                        gl_instance, # Pass the authenticated instance
                        proj_stub.id,
                        group.full_path,
                        token,
                        effective_gitlab_url,
                        hours_per_commit,
                        cfg_obj,
                        inter_repo_adaptive_delay_per_repo, # Pass inter-repo adaptive delay
                        dynamic_post_api_call_delay_seconds, # Pass dynamic per-API call delay
                        previous_scan_cache=previous_scan_cache, # Pass cache
                        current_commit_sha=current_commit_sha_for_cache, # Pass current SHA
                        num_workers=max_workers  
                    )
                    future_to_project_name[future] = proj_stub.path_with_namespace
            
            except GitlabListError as gl_list_err:
                logger.error(f"GitLab API error during initial project listing for group {group_path}. Processing submitted tasks. Details: {gl_list_err}")
            except Exception as ex_iter:
                logger.error(f"Unexpected error during initial project listing for group {group_path}: {ex_iter}. Processing submitted tasks.")

            for future in as_completed(future_to_project_name):
                project_name_for_log = future_to_project_name[future]
                try:
                    project_data_result = future.result()
                    if project_data_result:
                        if project_data_result.get("processing_status") == "skipped_fork":
                            pass # Already logged
                        else:
                            processed_repo_list.append(project_data_result)
                except Exception as exc:
                    logger.error(f"Project {project_name_for_log} generated an exception in its thread: {exc}", exc_info=True)
                    processed_repo_list.append({"name": project_name_for_log.split('/')[-1], 
                                                "organization": group.full_path, 
                                                "processing_error": f"Thread execution failed: {exc}"})

        logger.info(f"Finished processing for {project_count_for_group_submitted} projects from GitLab group: {group_path}. Collected {len(processed_repo_list)} results.")
        if repos_created_after_filter_date and skipped_by_date_filter_count > 0:
            logger.info(f"GitLab: Skipped {skipped_by_date_filter_count} non-public projects from group '{group_path}' due to the REPOS_CREATED_AFTER_DATE filter ('{repos_created_after_filter_date.strftime('%Y-%m-%d')}').")

    except GitlabAuthenticationError:
        logger.critical(f"GitLab authentication failed for URL {effective_gitlab_url}. Check token. Skipping GitLab scan for {group_path}.")
        # No need to append to processed_repo_list here, just return what we have or empty
    except GitlabGetError as e: # Error getting the initial group
        logger.critical(f"GitLab API error: Could not find group '{group_path}' on {effective_gitlab_url} or other API issue: {e.error_message} (Status: {e.response_code}). Skipping GitLab scan.", exc_info=False)
    except GitlabListError as e: # Error listing projects in the group
        logger.critical(f"GitLab API error listing projects for group '{group_path}' on {effective_gitlab_url}: {e.error_message}. Skipping GitLab scan.", exc_info=False)
    except Exception as e: # Catch-all for other unexpected errors during setup or group iteration
        logger.critical(f"An unexpected error occurred during GitLab connection or group processing for '{group_path}' on {effective_gitlab_url}: {e}", exc_info=True)

    return processed_repo_list


if __name__ == '__main__':
    # This basic test block will use environment variables for token, URL, and group
    # This is for direct testing of the connector, not via generate_codejson.py
    from dotenv import load_dotenv as load_dotenv_for_test # Alias to avoid conflict
    load_dotenv_for_test() # Load .env for test execution
    
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # For testing, get these from .env
    test_gl_token = os.getenv("GITLAB_TOKEN") 
    test_gl_url_env = os.getenv("GITLAB_URL", "https://gitlab.com")
    test_group_paths_str = os.getenv("GITLAB_GROUPS", "")
    test_group_path = test_group_paths_str.split(',')[0].strip() if test_group_paths_str else None

    if not test_gl_token or is_placeholder_token(test_gl_token): # Use the function with the token
        logger.error("Test GitLab token (GITLAB_TOKEN) not found or is a placeholder in .env.")
    elif not test_group_path:
        logger.error("No GitLab group found in GITLAB_GROUPS in .env for testing.")
    else:
        logger.info(f"--- Testing GitLab Connector for group: {test_group_path} on {test_gl_url_env} ---")
        counter = [0]
        counter_lock = threading.Lock()
        repositories = fetch_repositories(
            token=test_gl_token, 
            group_path=test_group_path, 
            processed_counter=counter, 
            processed_counter_lock=counter_lock,
            debug_limit=None, 
            gitlab_instance_url=test_gl_url_env,
            cfg_obj=None,
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
