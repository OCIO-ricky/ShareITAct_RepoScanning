# utils/labor_hrs_estimator.py
# These functions are designed to estimate labor hours based on repository commit history.
#
# Key Features:
# - Fetches commit history from GitHub, GitLab, and Azure DevOps repositories.
# - For API-based fetching (GitHub, GitLab, Azure DevOps), it internally uses asynchronous
#   operations (`aiohttp`) to retrieve commit pages concurrently. This speeds up the
#   process for repositories with many commits.
# - Despite the internal asynchronicity for commit fetching, the main analysis functions
#   (e.g., `analyze_github_repo_sync`) present a synchronous interface to their callers.
# - Can also analyze local Git repositories.
# - Calculates estimated labor hours based on the number of commits and a configurable
#   `hours_per_commit` rate.
# - Returns results as a pandas DataFrame, summarizing contributions by author.
#
# This module is typically called from within each platform-specific connector's
# `fetch_repositories()` function (or equivalent) when labor hour estimation is enabled..
import os
import subprocess
import requests
from datetime import datetime, timezone # Added timezone
import pandas as pd
import logging
import base64
import re # For parsing Link header
from typing import Optional, Dict, Any, List, Tuple, Union, Callable

import asyncio
import aiohttp

# Imports for robust session retries
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

ANSI_RED = "\033[91m"
ANSI_RESET = "\033[0m"

# Attempt to import the GitHubRateLimitHandler
try:
    from .rate_limit_handler import GitHubRateLimitHandler
except ImportError: # Fallback for different execution contexts
    GitHubRateLimitHandler = None
from .api_delayer import apply_post_api_call_delay
# It's good practice to have a logger instance per module
logger = logging.getLogger(__name__)
# --- Helper Functions ---
def _create_summary_dataframe(commit_records: List[Tuple[str, str, datetime]], hours_per_commit: float) -> pd.DataFrame:
    """
    Helper function to create a summary DataFrame from commit records.
    Each record in commit_records should be a tuple: (author_name, author_email, commit_date_datetime).
    """
    df_columns = ["Author", "Email", "Commits", "FirstCommit", "LastCommit", "EstimatedHours"]
    if not commit_records:
        logger.debug("No commit records provided; returning empty DataFrame.")
        return pd.DataFrame(columns=df_columns)

    try:
        df = pd.DataFrame(commit_records, columns=["Author", "Email", "Date"])
        if df.empty:
            return pd.DataFrame(columns=df_columns)

        df["EstimatedHours"] = hours_per_commit
        
        summary_df = df.groupby(["Author", "Email"], as_index=False).agg(
            Commits=("Date", "count"),
            FirstCommit=("Date", "min"),
            LastCommit=("Date", "max"),
            EstimatedHours=("EstimatedHours", "sum")
        )
        return summary_df
    except Exception as e:
        logger.error(f"Error creating summary DataFrame: {e}", exc_info=True)
        return pd.DataFrame(columns=df_columns)

def _get_azure_devops_auth_header_val(pat_token: str) -> Optional[str]:
    """Creates the Basic Authentication header value for Azure DevOps PAT."""
    if not pat_token:
        logger.error("Azure DevOps PAT token cannot be empty for Basic Authentication.")
        return None
    try:
        # The PAT itself is used as the password with an empty username for Basic Auth
        return "Basic " + base64.b64encode(f":{pat_token}".encode()).decode()
    except Exception as e:
        logger.error(f"{ANSI_RED}Failed to encode Azure DevOps PAT: {e}{ANSI_RESET}", exc_info=True)
        return None

def _parse_github_link_header(link_header: Optional[str]) -> Dict[str, str]:
    """Parses the GitHub Link header to find the 'next' page URL."""
    links = {}
    if link_header:
        parts = link_header.split(',')
        for part in parts:
            match = re.match(r'<(.*?)>; rel="(.*?)"', part.strip())
            if match:
                links[match.group(2)] = match.group(1)
    return links

def _create_resilient_session(base_url: Optional[str] = None) -> requests.Session:
    """
    Creates a requests.Session configured with a robust retry strategy.
    """
    session = requests.Session()
    retry_strategy = Retry(
        total=5,  # Total number of retries to allow
        backoff_factor=1,  # Base for exponential backoff (e.g., 1s, 2s, 4s, 8s, 16s)
        status_forcelist=[429, 500, 502, 503, 504],  # HTTP status codes to retry on
        # SSLErrors are connection errors, which are retried by default.
        # We can be explicit about allowed methods for retries:
        allowed_methods=frozenset(['HEAD', 'GET']),
        respect_retry_after_header=True
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    
    if base_url:
        session.mount(base_url, adapter)
    else: # Mount for both http and https if no specific base_url
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
    return session

async def _fetch_all_commit_pages_async(
    session: aiohttp.ClientSession,
    initial_url: str,
    platform_identifier_for_log: str,
    get_next_url_fn: Callable[[aiohttp.ClientResponse, List[Any]], Optional[str]],
    extract_items_from_page_fn: Callable[[Dict[str, Any]], List[Any]],
    process_commit_item_fn: Callable[[Dict[str, Any]], Optional[Tuple[str, str, datetime]]],
    rate_limit_handler: Optional[GitHubRateLimitHandler] = None, # Specifically for GitHub
    cfg_obj: Optional[Any] = None, # Configuration object
    num_repos_in_target_for_dynamic_delay: Optional[int] = None # For dynamic delay calculation
) -> List[Tuple[str, str, datetime]]:
    """
    Asynchronously fetches all pages for commits from a given platform API.
    """
    all_raw_commits: List[Dict[str, Any]] = []
    processed_commits: List[Tuple[str, str, datetime]] = []
    next_page_url: Optional[str] = initial_url
    page_data: Optional[Dict[str, Any]] = None # Initialize page_data

    while next_page_url:
        # Apply proactive delay and wait if rate limit is low (specifically for GitHub)
        if rate_limit_handler and platform_identifier_for_log.startswith("GitHub"):
            await rate_limit_handler.wait_if_critically_low()

        try:
            logger.debug(f"Async fetching commits for {platform_identifier_for_log} from: {next_page_url}")
            async with session.get(next_page_url, timeout=aiohttp.ClientTimeout(total=30)) as response:
                # Update rate limit status from headers (specifically for GitHub)
                if rate_limit_handler and platform_identifier_for_log.startswith("GitHub") and hasattr(rate_limit_handler, 'update_from_headers'):
                    # Ensure the handler has the latest info from GitHub
                    await rate_limit_handler.update_from_headers(response.headers)

                if response.status == 403: # GitHub API Rate Limit
                    error_text = await response.text()
                    try:
                        error_data = await response.json(content_type=None) # Try to parse JSON regardless of content type
                        if "API rate limit exceeded" in error_data.get("message", ""):
                            logger.warning(
                                f"GitHub API rate limit (403) hit during async fetch for {platform_identifier_for_log} from {next_page_url}. "
                                f"Stopping commit fetching for this repository. Details: {error_text}"
                            )
                            # If we have a rate_limit_handler, it should have tried to wait.
                            # This 403 might mean the wait wasn't enough, or it's a different 403.
                            if rate_limit_handler and platform_identifier_for_log.startswith("GitHub"):
                                logger.warning(f"This 403 for {platform_identifier_for_log} occurred despite rate limit handling. Headers might be stale or another issue.")
                            return [] # Return empty list, effectively stopping further processing for this repo
                    except Exception: # Includes JSONDecodeError or if error_data is not a dict
                        logger.error(f"{ANSI_RED}HTTP error 403 (but not a recognized rate limit message) for {platform_identifier_for_log} from {next_page_url}: {error_text}{ANSI_RESET}")
                    page_data = None # Ensure page_data is None to skip processing
                    break # Stop fetching for this repo on 403
                
                response.raise_for_status() # Raise for other 4xx/5xx errors
                if 'application/json' not in response.headers.get('Content-Type', '').lower():
                    error_text = await response.text()
                    logger.error(f"{ANSI_RED}Unexpected content type '{response.headers.get('Content-Type')}' for {platform_identifier_for_log} from {next_page_url}. Response: {error_text[:200]}{ANSI_RESET}")
                    page_data = None
                    break

                page_data = await response.json() # Assign to page_data
                page_items = extract_items_from_page_fn(page_data)

                if not page_items:
                    logger.debug(f"No items found on async page for {platform_identifier_for_log} from {next_page_url}. Assuming end of pagination.")
                    break
                
                all_raw_commits.extend(page_items)
                next_page_url = get_next_url_fn(response, page_items)
        except aiohttp.ClientResponseError as e:  # Handles raise_for_status()
            if e.status == 409 and platform_identifier_for_log.startswith("GitHub repository:"):
                logger.warning(
                    f"AIOHTTP ClientResponseError (409 Conflict) fetching commits for {platform_identifier_for_log} "
                    f"from {next_page_url}: {e.message}. This often indicates an empty repository."
                )
            else:  # For other ClientResponseErrors (e.g., 404, other 4xx, 5xx, or 409 for non-GitHub)
                logger.error(
                    f"{ANSI_RED}AIOHTTP ClientResponseError ({e.status}) fetching commits for {platform_identifier_for_log} from {next_page_url}: {e.message}{ANSI_RESET}", exc_info=False
                )
            page_data = None
            break
        except aiohttp.ClientError as e:
            logger.error(f"{ANSI_RED}AIOHTTP Client error fetching commits for {platform_identifier_for_log} from {next_page_url}: {e}{ANSI_RESET}", exc_info=True)
            page_data = None
            break
        except asyncio.TimeoutError:
            logger.error(f"{ANSI_RED}Timeout error fetching commits for {platform_identifier_for_log} from {next_page_url}{ANSI_RESET}", exc_info=True)
            page_data = None
            break
        except Exception as e: # Catch broader exceptions like JSONDecodeError (aiohttp.ContentTypeError)
            logger.error(f"{ANSI_RED}Error processing page for {platform_identifier_for_log} from {next_page_url}: {e}{ANSI_RESET}", exc_info=True)
            page_data = None
            break
        finally: # This block will execute after try/except, before next iteration or exit
            # Apply the generic post-API call delay for the current platform
            # This happens regardless of success or failure of the above API call,
            # as long as an attempt was made.
            platform_short_name = "UNKNOWN"
            if platform_identifier_for_log.startswith("GitHub"):
                platform_short_name = "GITHUB"
            elif platform_identifier_for_log.startswith("GitLab"):
                platform_short_name = "GITLAB"
            elif platform_identifier_for_log.startswith("Azure DevOps"):
                platform_short_name = "AZURE_DEVOPS"
            
            if platform_short_name != "UNKNOWN" and cfg_obj:
                await apply_post_api_call_delay(platform_short_name, cfg_obj, num_repos_in_target_for_dynamic_delay)

        if not page_data: # If page_data is None (e.g. due to an error handled above)
            break
    
    for commit_item_json in all_raw_commits:
        processed_item = process_commit_item_fn(commit_item_json)
        if processed_item:
            processed_commits.append(processed_item)
            
    if not processed_commits:
        logger.info(f"No commit records found or parsed via async fetch for {platform_identifier_for_log}")
    return processed_commits


def _fetch_all_commit_pages_sync( # This function is kept for potential future use or non-async scenarios
    session: requests.Session,
    initial_url: str,
    platform_identifier_for_log: str, # e.g., "GitHub owner/repo"
    get_next_url_fn: Callable[[requests.Response, List[Any]], Optional[str]],
    extract_items_from_page_fn: Callable[[Dict[str, Any]], List[Any]],
    process_commit_item_fn: Callable[[Dict[str, Any]], Optional[Tuple[str, str, datetime]]]
) -> List[Tuple[str, str, datetime]]:
    """
    Generic function to fetch and process commits from a paginated API. (Synchronous version)
    """
    all_raw_commits: List[Dict[str, Any]] = []
    processed_commits: List[Tuple[str, str, datetime]] = []
    next_page_url: Optional[str] = initial_url

    while next_page_url:
        try:
            logger.debug(f"Fetching commits for {platform_identifier_for_log} from: {next_page_url}")
            res = session.get(next_page_url, timeout=30)
            res.raise_for_status()
            
            page_json = res.json()
            page_items = extract_items_from_page_fn(page_json)

            if not page_items:
                logger.debug(f"No items found on page for {platform_identifier_for_log} from {next_page_url}. Assuming end of pagination.")
                break
            
            all_raw_commits.extend(page_items)
            next_page_url = get_next_url_fn(res, page_items)

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 403: 
                try:
                    error_data = e.response.json()
                    if "API rate limit exceeded" in error_data.get("message", ""):
                        logger.warning(
                            f"GitHub API rate limit exceeded for {platform_identifier_for_log} while fetching commits. "
                            f"Stopping commit fetching for this repository. Details: {e.response.text}"
                        )
                        return [] 
                except ValueError: 
                    pass 
            elif e.response.status_code == 409: 
                try:
                    error_data = e.response.json()
                    if error_data.get("message") == "Git Repository is empty.":
                        logger.info(f"{platform_identifier_for_log} is empty. No commits to analyze for labor hours.")
                        return []
                except ValueError: 
                    pass 
            logger.error(f"{ANSI_RED}HTTP error fetching commits for {platform_identifier_for_log} from {next_page_url}: {e.response.status_code} - {e.response.text}{ANSI_RESET}")
            break 
        except requests.exceptions.RequestException as e:
            logger.error(f"{ANSI_RED}Request error fetching commits for {platform_identifier_for_log} from {next_page_url}: {e}{ANSI_RESET}", exc_info=True)
            break
        except ValueError as e: # JSONDecodeError
            logger.error(f"{ANSI_RED}Error decoding JSON for {platform_identifier_for_log} from {next_page_url}: {e}{ANSI_RESET}", exc_info=True)
            break
    
    for commit_item_json in all_raw_commits:
        processed_item = process_commit_item_fn(commit_item_json)
        if processed_item:
            processed_commits.append(processed_item)
            
    if not processed_commits:
        logger.info(f"No commit records found or parsed for {platform_identifier_for_log}")
    return processed_commits

def _run_commit_fetching_asynchronously(
    base_api_url: str, # For logging or potential session configuration
    initial_url: str,
    platform_identifier_for_log: str,
    get_next_url_fn: Callable[[aiohttp.ClientResponse, List[Any]], Optional[str]],
    extract_items_from_page_fn: Callable[[Dict[str, Any]], List[Any]],
    process_commit_item_fn: Callable[[Dict[str, Any]], Optional[Tuple[str, str, datetime]]],
    auth_headers: Optional[Dict[str, str]] = None,
    rate_limit_handler: Optional[GitHubRateLimitHandler] = None, # Pass through for GitHub
    cfg_obj: Optional[Any] = None, # Pass through for post-call delays
    num_repos_in_target_for_dynamic_delay: Optional[int] = None # Pass through
) -> List[Tuple[str, str, datetime]]:
    """
    Synchronous wrapper to run the asynchronous commit fetching logic.
    """
    async def _main_async_logic():
        async with aiohttp.ClientSession(headers=auth_headers) as session: 
            return await _fetch_all_commit_pages_async(
                session,
                initial_url,
                platform_identifier_for_log,
                get_next_url_fn,
                extract_items_from_page_fn,
                process_commit_item_fn,
                rate_limit_handler=rate_limit_handler, # Pass through for GitHub
                cfg_obj=cfg_obj, # Pass through for post-call delays
                num_repos_in_target_for_dynamic_delay=num_repos_in_target_for_dynamic_delay # Pass through
            )
    try:
        return asyncio.run(_main_async_logic())
    except RuntimeError as e:
        logger.error(f"{ANSI_RED}RuntimeError during asyncio.run for {platform_identifier_for_log}: {e}. This might happen if called from an already running async event loop without proper nesting.{ANSI_RESET}", exc_info=True)
        return [] 

# --- Analysis Functions ---

def analyze_local_git(repo_path: str, hours_per_commit: float = 0.5) -> pd.DataFrame:
    """Estimate labor hours from a local Git repo."""
    logger.info(f"Analyzing local Git repository at: {repo_path}")
    log_format = "--pretty=format:%H|%an|%ae|%ad" # Hash|AuthorName|AuthorEmail|AuthorDate(iso)
    records: List[Tuple[str, str, datetime]] = []

    try:
        result = subprocess.run(
            ["git", "log", log_format, "--date=iso"],
            stdout=subprocess.PIPE,
            text=True,
            check=True,
            cwd=repo_path,
            encoding='utf-8'
        )
        log_data = result.stdout.strip().split('\n')

        for line in log_data:
            if not line.strip(): continue
            parts = line.split('|')
            if len(parts) == 4:
                _hash, author, email, date_str = parts
                try:
                    records.append((author.strip(), email.strip(), datetime.fromisoformat(date_str.strip())))
                except ValueError as ve:
                    logger.warning(f"Could not parse date '{date_str.strip()}' for commit by {author}. Skipping. Error: {ve}")
            else:
                logger.warning(f"Malformed git log line in {repo_path}: '{line}'. Skipping.")
    
    except FileNotFoundError:
        logger.error(f"{ANSI_RED}Git command not found or '{repo_path}' is not a valid directory. Ensure Git is installed and in PATH.{ANSI_RESET}")
    except subprocess.CalledProcessError as e:
        logger.error(f"{ANSI_RED}Error running 'git log' in '{repo_path}': {e.stderr or e.stdout or e}{ANSI_RESET}")
    except Exception as e:
        logger.error(f"{ANSI_RED}Unexpected error analyzing local git repo '{repo_path}':{ANSI_RESET} {e}", exc_info=True)

    if not records:
        logger.info(f"No commit records found or parsed for local repo: {repo_path}")
    return _create_summary_dataframe(records, hours_per_commit)


def analyze_github_repo_sync(
    owner: str, 
    repo: str, 
    token: str, 
    hours_per_commit: Optional[float] = 0.5, 
    github_api_url: str = "https://api.github.com",
    session: Optional[requests.Session] = None, # This session is for synchronous calls if any were needed
    cfg_obj: Optional[Any] = None, 
    num_repos_in_target: Optional[int] = None, # For dynamic delay in commit fetching
    is_empty_repo: bool = False # Added to proactively skip empty repos
) -> pd.DataFrame:
    """Estimate labor hours from a GitHub repo using its API (asynchronously for commit fetching)."""
    platform_identifier = f"GitHub repository: {owner}/{repo}"

    if is_empty_repo:
        logger.info(f"Repository {platform_identifier} is marked as empty. Skipping labor hours estimation.")
        return _create_summary_dataframe([], 0.0)

    if hours_per_commit is None:
        logger.info(f"hours_per_commit is None for GitHub repo {owner}/{repo}. "
                      "Skipping labor hours estimation and returning empty DataFrame.")
        return _create_summary_dataframe([], 0.0)
    
    logger.info(f"Analyzing {platform_identifier} for labor hours using API: {github_api_url}")
    
    # --- GitHub Rate Limit Handling ---
    active_rate_limit_handler = None
    if GitHubRateLimitHandler: # Check if class was imported
        # Parameters for GitHubRateLimitHandler can be made configurable here if needed
        # For now, using its internal defaults.
        # Example if you wanted to configure them from cfg_obj or os.getenv:
        # safety_buffer = int(getattr(cfg_obj, 'GITHUB_RL_SAFETY_BUFFER_ENV', os.getenv("GITHUB_RL_SAFETY_BUFFER", "10")))
        # min_sleep = float(getattr(cfg_obj, 'GITHUB_RL_MIN_SLEEP_SECONDS_ENV', os.getenv("GITHUB_RL_MIN_SLEEP_SECONDS", "1.0")))
        # max_sleep = float(getattr(cfg_obj, 'GITHUB_RL_MAX_SLEEP_SECONDS_ENV', os.getenv("GITHUB_RL_MAX_SLEEP_SECONDS", "3600.0")))
        # active_rate_limit_handler = GitHubRateLimitHandler(
        #     safety_buffer_remaining=safety_buffer, min_sleep_if_limited=min_sleep, max_sleep_duration=max_sleep
        # )
        active_rate_limit_handler = GitHubRateLimitHandler() # Using defaults from the class
        logger.info(f"GitHubRateLimitHandler activated for {platform_identifier}.")
    else:
        logger.warning("GitHubRateLimitHandler class not available. GitHub API calls will not have advanced rate limit handling.")

    _session_managed_internally = False # For the synchronous session, if used
    if session is None:
        session = _create_resilient_session(base_url=github_api_url)
        _session_managed_internally = True
        logger.debug("No external synchronous session provided; created internal resilient session for GitHub (if needed).")
    else:
        logger.debug("Using externally provided synchronous session for GitHub (if needed).")

    initial_url = f"{github_api_url.rstrip('/')}/repos/{owner}/{repo}/commits?per_page=100"

    def _github_get_next_url(response: Union[requests.Response, aiohttp.ClientResponse], page_items: List[Any]) -> Optional[str]:
        link_header = response.headers.get("Link")
        links = _parse_github_link_header(link_header)
        return links.get("next")

    def _github_extract_items(page_json: Union[List[Any], Dict[str, Any]]) -> List[Any]:
        return page_json if isinstance(page_json, list) else [] 

    def _github_process_item(commit_item: Dict[str, Any]) -> Optional[Tuple[str, str, datetime]]:
        commit_details = commit_item.get("commit")
        if commit_details:
            author_info = commit_details.get("author", {})
            author_name = author_info.get("name", "Unknown Author")
            author_email = author_info.get("email", "unknown@example.com")
            date_str = author_info.get("date")
            if date_str:
                try:
                    commit_date = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                    return (author_name, author_email, commit_date)
                except ValueError as ve:
                    logger.warning(f"Could not parse date '{date_str}' for GitHub commit. Error: {ve}")
        return None

    processed_commits: List[Tuple[str, str, datetime]] = []
    
    auth_headers_for_github = {}
    if token:
        auth_headers_for_github["Authorization"] = f"token {token}"
    auth_headers_for_github.setdefault("Accept", "application/vnd.github.v3+json")

    try:
        processed_commits = _run_commit_fetching_asynchronously(
            base_api_url=github_api_url,
            initial_url=initial_url,
            platform_identifier_for_log=platform_identifier,
            get_next_url_fn=_github_get_next_url,
            extract_items_from_page_fn=_github_extract_items,
            process_commit_item_fn=_github_process_item,
            auth_headers=auth_headers_for_github,
            rate_limit_handler=active_rate_limit_handler, # Pass the GitHub-specific handler
            cfg_obj=cfg_obj, # Pass config for generic post-call delays
            num_repos_in_target_for_dynamic_delay=num_repos_in_target
        )
    finally:
        if _session_managed_internally and session:
            session.close()
            logger.debug(f"Closed internally managed synchronous session for {platform_identifier}.")
            
    return _create_summary_dataframe(processed_commits, hours_per_commit)


def analyze_gitlab_repo_sync(
    project_id: Union[str, int], 
    token: str, 
    hours_per_commit: Optional[float] = 0.5, 
    gitlab_api_url: str = "https://gitlab.com",
    session: Optional[requests.Session] = None, 
    cfg_obj: Optional[Any] = None,
    num_repos_in_target: Optional[int] = None, # For dynamic delay in commit fetching
    is_empty_repo: bool = False # Added to proactively skip empty repos
) -> pd.DataFrame:
    """Estimate labor hours from a GitLab repo using its API (asynchronously for commit fetching)."""
    platform_identifier = f"GitLab project ID: {project_id}"

    if is_empty_repo:
        logger.info(f"Repository {platform_identifier} is marked as empty. Skipping labor hours estimation.")
        return _create_summary_dataframe([], 0.0)

    if hours_per_commit is None:
        logger.info(f"hours_per_commit is None for {platform_identifier}. "
                      "Skipping labor hours estimation and returning empty DataFrame.")
        return _create_summary_dataframe([], 0.0)

    logger.info(f"Analyzing {platform_identifier} for labor hours using API: {gitlab_api_url}")

    _session_managed_internally = False 
    if session is None:
        session = _create_resilient_session(base_url=gitlab_api_url)
        _session_managed_internally = True
        logger.debug(f"No external synchronous session provided; created internal resilient session for {platform_identifier} (if needed).")
    else:
        logger.debug(f"Using externally provided synchronous session for {platform_identifier} (if needed).")

    per_page = 100
    initial_url = f"{gitlab_api_url.rstrip('/')}/api/v4/projects/{project_id}/repository/commits?page=1&per_page={per_page}"

    def _gitlab_get_next_url(response: Union[requests.Response, aiohttp.ClientResponse], page_items: List[Any]) -> Optional[str]:
        next_page_header = response.headers.get("X-Next-Page")
        if next_page_header and next_page_header.strip():
            return f"{gitlab_api_url.rstrip('/')}/api/v4/projects/{project_id}/repository/commits?page={next_page_header.strip()}&per_page={per_page}"
        if len(page_items) < per_page: 
            return None
        logger.debug(f"No X-Next-Page header but full page returned for {platform_identifier}, assuming last page.")
        return None

    def _gitlab_extract_items(page_json: Union[List[Any], Dict[str, Any]]) -> List[Any]:
        return page_json if isinstance(page_json, list) else []

    def _gitlab_process_item(commit_item: Dict[str, Any]) -> Optional[Tuple[str, str, datetime]]:
        author_name = commit_item.get("author_name", "Unknown Author")
        author_email = commit_item.get("author_email", "unknown@example.com")
        date_str = commit_item.get("created_at")
        if date_str:
            try:
                commit_date = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                return (author_name, author_email, commit_date)
            except ValueError as ve:
                logger.warning(f"Could not parse date '{date_str}' for GitLab commit. Error: {ve}")
        return None

    processed_commits: List[Tuple[str, str, datetime]] = []
    auth_headers_for_gitlab = {"PRIVATE-TOKEN": token} if token else {}

    try:
        processed_commits = _run_commit_fetching_asynchronously(
            base_api_url=gitlab_api_url,
            initial_url=initial_url,
            platform_identifier_for_log=platform_identifier,
            get_next_url_fn=_gitlab_get_next_url,
            extract_items_from_page_fn=_gitlab_extract_items,
            process_commit_item_fn=_gitlab_process_item,
            auth_headers=auth_headers_for_gitlab,
            cfg_obj=cfg_obj, # Pass config for generic post-call delays
            num_repos_in_target_for_dynamic_delay=num_repos_in_target
        )
    finally:
        if _session_managed_internally and session:
            session.close()
            logger.debug(f"Closed internally managed synchronous session for {platform_identifier}.")

    return _create_summary_dataframe(processed_commits, hours_per_commit)


def analyze_azure_devops_repo_sync(
    organization: str, 
    project: str, 
    repo_id: str, 
    pat_token: str, 
    hours_per_commit: Optional[float] = 0.5,
    azure_devops_api_url: str = "https://dev.azure.com",
    session: Optional[requests.Session] = None, 
    cfg_obj: Optional[Any] = None,
    num_repos_in_target: Optional[int] = None, # For dynamic delay in commit fetching
    is_empty_repo: bool = False # Added to proactively skip empty repos
) -> pd.DataFrame:
    """Estimate labor hours from Azure DevOps repo using its API (asynchronously for commit fetching)."""
    platform_identifier = f"Azure DevOps repository: {organization}/{project}/{repo_id}"
    
    if is_empty_repo:
        logger.info(f"Repository {platform_identifier} is marked as empty. Skipping labor hours estimation.")
        return _create_summary_dataframe([], 0.0)

    if hours_per_commit is None:
        logger.info(f"hours_per_commit is None for Azure DevOps repo {organization}/{project}/{repo_id}. "
                      "Skipping labor hours estimation and returning empty DataFrame.")
        return _create_summary_dataframe([], 0.0)

    logger.info(f"Analyzing {platform_identifier} for labor hours using API: {azure_devops_api_url}")

    _session_managed_internally = False 
    if session is None:
        session = _create_resilient_session(base_url=azure_devops_api_url)
        _session_managed_internally = True
        logger.debug(f"No external synchronous session provided; created internal resilient session for {platform_identifier} (if needed).")
    else:
        logger.debug(f"Using externally provided synchronous session for {platform_identifier} (if needed).")

    top = 100
    api_version = "6.0" 
    _current_skip = 0 
    initial_url = (f"{azure_devops_api_url.rstrip('/')}/{organization}/{project}/_apis/git/repositories/{repo_id}"
                   f"/commits?api-version={api_version}&$top={top}&$skip={_current_skip}")

    def _ado_get_next_url(response: Union[requests.Response, aiohttp.ClientResponse], page_items: List[Any]) -> Optional[str]:
        nonlocal _current_skip # Ensure we modify the outer scope _current_skip
        if len(page_items) < top: 
            return None
        _current_skip += top
        return (f"{azure_devops_api_url.rstrip('/')}/{organization}/{project}/_apis/git/repositories/{repo_id}"
                f"/commits?api-version={api_version}&$top={top}&$skip={_current_skip}")
    
    def _ado_extract_items(page_json: Union[List[Any], Dict[str, Any]]) -> List[Any]:
        return page_json.get("value", []) if isinstance(page_json, dict) else []

    def _ado_process_item(commit_item: Dict[str, Any]) -> Optional[Tuple[str, str, datetime]]:
        author_info = commit_item.get("author", {})
        author_name = author_info.get("name", "Unknown Author")
        author_email = author_info.get("email", "unknown@example.com")
        date_str = author_info.get("date")
        if date_str:
            try:
                commit_date = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                return (author_name, author_email, commit_date)
            except ValueError as ve:
                logger.warning(f"Could not parse date '{date_str}' for {platform_identifier} commit. Error: {ve}")
        return None

    processed_commits: List[Tuple[str, str, datetime]] = []
    auth_headers_for_ado = {}
    if pat_token:
        auth_val = _get_azure_devops_auth_header_val(pat_token)
        if auth_val:
            auth_headers_for_ado["Authorization"] = auth_val

    try:
        processed_commits = _run_commit_fetching_asynchronously(
            base_api_url=azure_devops_api_url,
            initial_url=initial_url,
            platform_identifier_for_log=platform_identifier,
            get_next_url_fn=_ado_get_next_url,
            extract_items_from_page_fn=_ado_extract_items,
            process_commit_item_fn=_ado_process_item,
            auth_headers=auth_headers_for_ado,
            cfg_obj=cfg_obj, # Pass config for generic post-call delays
            num_repos_in_target_for_dynamic_delay=num_repos_in_target
        )
    finally:
        if _session_managed_internally and session:
            session.close()
            logger.debug(f"Closed internally managed synchronous session for {platform_identifier}.")
            
    return _create_summary_dataframe(processed_commits, hours_per_commit)
