# utils/labor_hrs_estimator.py
"""
Estimates labor hours for software projects based on commit history.

Key Features:
- Supports GitHub, GitLab, and Azure DevOps repositories.
- For GitHub and GitLab, it now uses GraphQL to fetch commit history, aiming
  to reduce API call volume and improve efficiency.
- For Azure DevOps, it continues to use REST APIs (via aiohttp) as a
  GraphQL equivalent for commit history is not available.
- The primary analysis functions (`analyze_github_repo_sync`, etc.) present a
  synchronous interface to their callers, managing any internal async operations.
- Can also analyze local Git repositories.
- Calculates estimated labor hours based on the number of commits and a
  configurable `hours_per_commit` rate. Fetches a maximum of 5000 commits
  per repository for estimation.
- Returns results as a pandas DataFrame, summarizing contributions by author.

This module is typically called from within each platform-specific connector's
processing function when labor hour estimation is enabled.
"""
import os
import subprocess
from datetime import datetime, timezone
import pandas as pd
import logging
import asyncio # For ADO async logic
import re # For parsing Link header (though less used now)
from typing import Optional, Dict, Any, List, Tuple, Union, Callable

# Attempt to import aiohttp for Azure DevOps async operations
try:
    import aiohttp
    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False
    aiohttp = None # Ensure aiohttp is defined for type hints even if not available
    logging.getLogger(__name__).warning(
        "Failed to import aiohttp. Azure DevOps labor estimation via async API calls will be skipped."
    )
# Attempt to import GraphQL clients
try:
    from clients.graphql_clients import github_gql, gitlab_gql
    from gql import Client as GqlClient # For type hinting GitHub client
    import gitlab as python_gitlab_library # For type hinting GitLab instance
    GRAPHQL_CLIENTS_AVAILABLE = True
except ImportError:
    GRAPHQL_CLIENTS_AVAILABLE = False
    github_gql, gitlab_gql, GqlClient, python_gitlab_library = None, None, None, None
    logging.getLogger(__name__).warning(
        "Failed to import GraphQL clients for labor_hrs_estimator. "
        "GitHub/GitLab labor estimation via GQL will be skipped."
    )

ANSI_RED = "\033[91m"
ANSI_RESET = "\033[0m"

logger = logging.getLogger(__name__)

# --- Helper Functions ---

def _parse_iso_datetime_str(date_str: Optional[str], logger_to_use: logging.Logger, org_group_context: str, context_msg: str = "") -> Optional[datetime]:
    """Safely parses an ISO datetime string, handling potential milliseconds and Z timezone."""
    if not date_str:
        return None
    try:
        # Handle potential milliseconds by stripping them before parsing if they exist
        # and ensure 'Z' is replaced with +00:00 for fromisoformat
        processed_date_str = date_str.split('.')[0] if '.' in date_str else date_str
        processed_date_str = processed_date_str.replace("Z", "+00:00")
        return datetime.fromisoformat(processed_date_str)
    except ValueError as ve:
        logger_to_use.warning(f"Could not parse date string '{date_str}'{context_msg}. Error: {ve}", extra={'org_group': org_group_context})
        return None

def _create_summary_dataframe(
    commits_data: List[Tuple[str, str, datetime]],
    hours_per_commit: float
) -> pd.DataFrame:
    """
    Creates a pandas DataFrame summarizing commits by author and estimated hours.
    commits_data: List of (author_name, author_email, commit_date)
    """
    if not commits_data:
        return pd.DataFrame(columns=['Author', 'Email', 'Commits', 'EstimatedHours'])

    df = pd.DataFrame(commits_data, columns=['Author', 'Email', 'Date'])
    df['Author'] = df['Author'].fillna('Unknown Author')
    df['Email'] = df['Email'].fillna('unknown@example.com')
    
    # Group by Author and Email, then count commits and calculate hours
    summary_df = df.groupby(['Author', 'Email']).agg(
        Commits=('Date', 'count')
    ).reset_index()
    
    summary_df['EstimatedHours'] = summary_df['Commits'] * hours_per_commit
    return summary_df

def _get_azure_devops_auth_header_val(pat_token: str) -> Optional[str]:
    """
    Encodes the Azure DevOps PAT for the Authorization header.
    Returns the header value string or None if encoding fails.
    """
    if not pat_token:
        logger.error(f"{ANSI_RED}Azure DevOps PAT is missing.{ANSI_RESET}")
        return None
    try:
        # For PAT, the username part is empty, and the token is the password part.
        # The string to encode is ":<PAT>"
        pat_for_b64 = f":{pat_token}"
        pat_b64_bytes = base64.b64encode(pat_for_b64.encode('utf-8'))
        return f"Basic {pat_b64_bytes.decode('utf-8')}"
    except Exception as e:
        logger.error(f"{ANSI_RED}Failed to encode Azure DevOps PAT: {e}{ANSI_RESET}", exc_info=True)
        return None

# --- Analysis Functions ---

def analyze_local_repo(
    repo_path: str, 
    hours_per_commit: Optional[float] = 0.5,
    logger_instance: Optional[logging.LoggerAdapter] = None) -> pd.DataFrame:
    """
    Analyzes a local Git repository for commit history and estimates labor hours.
    """
    platform_identifier = f"Local Git repository: {repo_path}"
    if hours_per_commit is None or hours_per_commit <= 0:
        logger.info(f"hours_per_commit is not set or invalid for {platform_identifier}. "
                      "Skipping labor hours estimation and returning empty DataFrame.", extra={'org_group': platform_identifier})
        return _create_summary_dataframe([], 0.0)

    current_logger = logger_instance if logger_instance else logger
    current_logger.info(f"Analyzing {platform_identifier} for labor hours.", extra={'org_group': platform_identifier})
    processed_commits: List[Tuple[str, str, datetime]] = []
    try:
        # Using --no-pager to prevent interactive prompts if output is long
        # Format: author-name<SEP>author-email<SEP>author-date-iso8601
        # <SEP> is a unique separator unlikely to appear in names/emails
        sep = "<|SEP|>"
        log_format = f"--pretty=format:%an{sep}%ae{sep}%aI" # %aI is author date, ISO 8601 format
        # Fetch a maximum of 5000 commits for performance reasons
        git_log_command = ["git", "-C", repo_path, "log", log_format, "--max-count=5000", "--no-pager"]
        
        result = subprocess.run(git_log_command, capture_output=True, text=True, check=False, encoding='utf-8')

        if result.returncode != 0:
            error_message = result.stderr.strip()
            if "does not have any commits yet" in error_message or \
               "does not have any references" in error_message or \
               "your current branch 'master' does not have any commits yet" in error_message:
                current_logger.info(f"{platform_identifier} is empty or has no commits. No labor hours to estimate.", extra={'org_group': platform_identifier})
            else:
                current_logger.error(f"Error running git log for {platform_identifier}: {error_message}", extra={'org_group': platform_identifier})
            return _create_summary_dataframe([], 0.0)

        for line in result.stdout.strip().split('\n'):
            if not line: continue
            parts = line.split(sep)
            if len(parts) == 3:
                author_name, author_email, date_str = parts
                commit_date = _parse_iso_datetime_str(date_str, current_logger, platform_identifier, f" for local git commit (Author: {author_name})")
                if commit_date:
                    processed_commits.append((author_name, author_email, commit_date))
            else:
                current_logger.warning(f"Unexpected git log line format for {platform_identifier}: {line}", extra={'org_group': platform_identifier})
                
    except FileNotFoundError:
        current_logger.error(f"Git command not found. Please ensure Git is installed and in your PATH for {platform_identifier}.", extra={'org_group': platform_identifier})
    except subprocess.CalledProcessError as e: # Should be caught by check=False and returncode check
        current_logger.error(f"Git log command failed for {platform_identifier}: {e.stderr}", extra={'org_group': platform_identifier})
    except Exception as e:
        current_logger.error(f"An unexpected error occurred while analyzing {platform_identifier}: {e}", exc_info=True, extra={'org_group': platform_identifier})

    return _create_summary_dataframe(processed_commits, hours_per_commit)


def analyze_github_repo_sync(
    owner: str, 
    repo: str, 
    token: str, 
    hours_per_commit: Optional[float] = 0.5, 
    github_api_url: str = "https://api.github.com", # Used for constructing GQL client endpoint
    default_branch_override: Optional[str] = None, # Added to accept specific default branch
    cfg_obj: Optional[Any] = None, 
    num_repos_in_target: Optional[int] = None,
    is_empty_repo: bool = False,
    number_of_workers: int = 1, # For logging/context if needed
    logger_instance: Optional[logging.LoggerAdapter] = None
) -> pd.DataFrame:
    """
    Analyzes a GitHub repository for commit history using GraphQL and estimates labor hours.
    """
    platform_identifier = f"GitHub repository: {owner}/{repo}"
    current_logger = logger_instance if logger_instance else logger

    if hours_per_commit is None or hours_per_commit <= 0:
        current_logger.info(f"hours_per_commit is not set or invalid for {platform_identifier}. "
                      "Skipping labor hours estimation and returning empty DataFrame.", extra={'org_group': platform_identifier})
        return _create_summary_dataframe([], 0.0)
    if is_empty_repo:
        current_logger.info(f"{platform_identifier} is marked as empty. "
                      "Skipping labor hours estimation and returning empty DataFrame.", extra={'org_group': platform_identifier})
        return _create_summary_dataframe([], 0.0)
    
    current_logger.info(f"Analyzing {platform_identifier} for labor hours using GraphQL.", extra={'org_group': platform_identifier})

    if not GRAPHQL_CLIENTS_AVAILABLE or not github_gql:
        current_logger.error(f"GitHub GraphQL client not available for {platform_identifier}. Skipping labor estimation.", extra={'org_group': platform_identifier})
        return _create_summary_dataframe([], 0.0)
    
    base_url_for_gql_client: Optional[str] = None
    if github_api_url and github_api_url.strip():
        if "api.github.com" not in github_api_url.lower(): # Indicates GHES
            base_url_for_gql_client = github_api_url.rstrip('/').replace('/api/v3', '').rstrip('/')
    gql_client: Optional[GqlClient] = None
    try:
        gql_client = github_gql.get_github_gql_client(token, base_url_for_gql_client)
    except Exception as e_client:
        current_logger.error(f"Failed to initialize GitHub GraphQL client for {platform_identifier}: {e_client}", exc_info=True, extra={'org_group': platform_identifier})
        return _create_summary_dataframe([], 0.0)

    processed_commits: List[Tuple[str, str, datetime]] = []
    if gql_client:
        commit_data_tuples = github_gql.fetch_commit_history_graphql(
            client=gql_client,
            owner=owner,
            repo_name=repo,
            default_branch_name=default_branch_override,
            logger_instance=current_logger # Pass logger
        )
        for name, email, date_str in commit_data_tuples:
            commit_date = _parse_iso_datetime_str(date_str, current_logger, platform_identifier, f" for GitHub commit (Author: {name})")
            if commit_date:
                processed_commits.append((name, email, commit_date))

    return _create_summary_dataframe(processed_commits, hours_per_commit)


def analyze_gitlab_repo_sync(
    project_id: str, # GitLab project ID (numeric string)
    token: str, 
    hours_per_commit: Optional[float] = 0.5, 
    gitlab_api_url: str = "https://gitlab.com",
    gl_instance_for_gql: Optional[python_gitlab_library.Gitlab] = None, # Accept an existing client
    default_branch_override: Optional[str] = None,
    cfg_obj: Optional[Any] = None,
    num_repos_in_target: Optional[int] = None,
    is_empty_repo: bool = False,
    number_of_workers: int = 1, # For logging/context if needed
    logger_instance: Optional[logging.LoggerAdapter] = None
) -> pd.DataFrame:
    """
    Analyzes a GitLab repository for commit history using GraphQL and estimates labor hours.
    """
    platform_identifier = f"GitLab project ID: {project_id}"
    current_logger = logger_instance if logger_instance else logger

    if hours_per_commit is None or hours_per_commit <= 0:
        current_logger.info(f"hours_per_commit is not set or invalid for {platform_identifier}. "
                      "Skipping labor hours estimation and returning empty DataFrame.", extra={'org_group': platform_identifier})
        return _create_summary_dataframe([], 0.0)
    if is_empty_repo:
        current_logger.info(f"{platform_identifier} is marked as empty. "
                      "Skipping labor hours estimation and returning empty DataFrame.", extra={'org_group': platform_identifier})
        return _create_summary_dataframe([], 0.0)

    current_logger.info(f"Analyzing {platform_identifier} for labor hours using GraphQL.", extra={'org_group': platform_identifier})

    if not GRAPHQL_CLIENTS_AVAILABLE or not gitlab_gql or not python_gitlab_library:
        current_logger.error(f"GitLab GraphQL client or python-gitlab library not available for {platform_identifier}. Skipping labor estimation.", extra={'org_group': platform_identifier})
        return _create_summary_dataframe([], 0.0)

    gl_instance_to_use = gl_instance_for_gql 
    if not gl_instance_to_use:
        current_logger.debug(f"No existing GitLab client passed for {platform_identifier}, creating a new one for labor estimation.", extra={'org_group': platform_identifier})
        try:
            ssl_verify_flag = os.getenv("DISABLE_SSL_VERIFICATION", "false").lower() != "true"
            gl_instance_to_use = python_gitlab_library.Gitlab(gitlab_api_url.strip('/'), private_token=token, ssl_verify=ssl_verify_flag, timeout=30)
            gl_instance_to_use.auth() 
        except Exception as e_client:
            current_logger.error(f"Failed to initialize new GitLab client for {platform_identifier}: {e_client}", exc_info=True, extra={'org_group': platform_identifier})
            return _create_summary_dataframe([], 0.0)
    processed_commits: List[Tuple[str, str, datetime]] = []
    if gl_instance_to_use:
        project_full_path = None
        actual_default_branch_for_commits = default_branch_override
        try:
            # This REST call is necessary to get project_full_path for the GQL query
            # and to confirm/get the default branch if not overridden.
            project_obj = gl_instance_to_use.projects.get(project_id) 
            project_full_path = project_obj.path_with_namespace
            if not actual_default_branch_for_commits: 
                actual_default_branch_for_commits = project_obj.default_branch
        except Exception as e_proj_get:
            current_logger.error(f"Failed to get project details (full_path, default_branch) for GitLab project ID {project_id}: {e_proj_get}", extra={'org_group': platform_identifier})
            
        if project_full_path:
            commit_data_tuples = gitlab_gql.fetch_commit_history_graphql(
                gl_instance=gl_instance_to_use, 
                project_full_path=project_full_path,
                default_branch_name=actual_default_branch_for_commits,
                logger_instance=current_logger # Pass logger
            )
            for name, email, date_str in commit_data_tuples:
                commit_date = _parse_iso_datetime_str(date_str, current_logger, platform_identifier, f" for GitLab commit (Author: {name})")
                if commit_date:
                    processed_commits.append((name, email, commit_date))

    return _create_summary_dataframe(processed_commits, hours_per_commit)


def analyze_azure_devops_repo_sync(
    organization: str, 
    project: str, 
    repo_id: str, 
    pat_token: str, 
    hours_per_commit: Optional[float] = 0.5,
    azure_devops_api_url: str = "https://dev.azure.com",
    cfg_obj: Optional[Any] = None,
    num_repos_in_target: Optional[int] = None,
    is_empty_repo: bool = False,
    number_of_workers: int = 1, # For logging/context if needed
    logger_instance: Optional[logging.LoggerAdapter] = None
) -> pd.DataFrame:
    """
    Analyzes an Azure DevOps repository for commit history using REST API (aiohttp)
    and estimates labor hours.
    """
    platform_identifier = f"Azure DevOps repository: {organization}/{project}/{repo_id}"
    current_logger = logger_instance if logger_instance else logger

    if hours_per_commit is None or hours_per_commit <= 0:
        current_logger.info(f"hours_per_commit is not set or invalid for {platform_identifier}. "
                      "Skipping labor hours estimation and returning empty DataFrame.", extra={'org_group': platform_identifier})
        return _create_summary_dataframe([], 0.0)
    if is_empty_repo:
        current_logger.info(f"{platform_identifier} is marked as empty. "
                      "Skipping labor hours estimation and returning empty DataFrame.", extra={'org_group': platform_identifier})
        return _create_summary_dataframe([], 0.0)
    if not AIOHTTP_AVAILABLE:
        current_logger.error(f"aiohttp library not available. Skipping Azure DevOps labor estimation for {platform_identifier}.", extra={'org_group': platform_identifier})
        return _create_summary_dataframe([], 0.0)

    current_logger.info(f"Analyzing {platform_identifier} for labor hours using API: {azure_devops_api_url}", extra={'org_group': platform_identifier})

    top = 100 # ADO API items per page
    api_version = "6.0" 
    
    def _ado_process_item(commit_item: Dict[str, Any]) -> Optional[Tuple[str, str, datetime]]:
        author_info = commit_item.get("author", {})
        author_name = author_info.get("name", "Unknown Author")
        author_email = author_info.get("email", "unknown@example.com")
        date_str = author_info.get("date") # This is usually committer date for ADO
        if not date_str: date_str = commit_item.get("committer", {}).get("date") # Fallback

        if date_str:
            commit_date = _parse_iso_datetime_str(date_str, current_logger, platform_identifier, " for Azure DevOps commit")
            if commit_date:
                return (author_name, author_email, commit_date)
        return None

    auth_headers_for_ado = {}
    auth_val = _get_azure_devops_auth_header_val(pat_token)
    if auth_val:
        auth_headers_for_ado["Authorization"] = auth_val
    else: # No valid auth header, cannot proceed
        current_logger.error(f"Failed to create auth header for Azure DevOps {platform_identifier}. Skipping commit fetch.", extra={'org_group': platform_identifier})
        return _create_summary_dataframe([], 0.0)

    async def fetch_ado_commits_async():
        commits_data: List[Tuple[str, str, datetime]] = []
        _current_skip_async = 0 
        max_commits_to_fetch = 5000 # Safety limit
        fetched_count = 0

        try:
            from utils.api_delayer import apply_post_api_call_delay 
        except ImportError:
            current_logger.error("Required import for ADO async fetch (api_delayer or aiohttp) missing.", extra={'org_group': platform_identifier}) # Should be caught by AIOHTTP_AVAILABLE
            return []

        async with aiohttp.ClientSession(headers=auth_headers_for_ado) as aio_session:
            while fetched_count < max_commits_to_fetch:
                current_url_to_fetch = (f"{azure_devops_api_url.rstrip('/')}/{organization}/{project}/_apis/git/repositories/{repo_id}"
                                       f"/commits?api-version={api_version}&$top={top}&$skip={_current_skip_async}")
                current_logger.debug(f"Async fetching ADO commits for {platform_identifier} from: {current_url_to_fetch}", extra={'org_group': platform_identifier})
                try:
                    async with aio_session.get(current_url_to_fetch, timeout=aiohttp.ClientTimeout(total=30)) as response:
                        response.raise_for_status()
                        page_json = await response.json()
                        page_items = page_json.get("value", [])

                        if not page_items:
                            break 
                        
                        for item in page_items:
                            if fetched_count >= max_commits_to_fetch: break
                            processed = _ado_process_item(item)
                            if processed:
                                commits_data.append(processed)
                                fetched_count += 1
                        
                        if len(page_items) < top or fetched_count >= max_commits_to_fetch:
                            break 
                        else:
                            _current_skip_async += top
                except aiohttp.ClientResponseError as e:
                    current_logger.error(f"AIOHTTP ClientResponseError ({e.status}) for ADO {platform_identifier} from {current_url_to_fetch}: {e.message}", extra={'org_group': platform_identifier})
                    break
                except Exception as e:
                    current_logger.error(f"Error processing ADO commit page for {platform_identifier} from {current_url_to_fetch}: {e}", exc_info=True, extra={'org_group': platform_identifier})
                    break
                finally:
                    if cfg_obj: 
                        await apply_post_api_call_delay("AZURE_DEVOPS", cfg_obj, num_repos_in_target)
        current_logger.info(f"Fetched a total of {len(commits_data)} commit data entries for ADO repo {platform_identifier} via aiohttp.", extra={'org_group': platform_identifier})
        return commits_data

    processed_commits: List[Tuple[str, str, datetime]] = []
    try:
        processed_commits = asyncio.run(fetch_ado_commits_async())
    except RuntimeError as e:
        current_logger.error(f"RuntimeError running async ADO commit fetch for {platform_identifier}: {e}. This might be due to nested event loops.", extra={'org_group': platform_identifier})
    except Exception as e_outer:
        current_logger.error(f"Outer exception running async ADO commit fetch for {platform_identifier}: {e_outer}", exc_info=True, extra={'org_group': platform_identifier})
            
    return _create_summary_dataframe(processed_commits, hours_per_commit)
