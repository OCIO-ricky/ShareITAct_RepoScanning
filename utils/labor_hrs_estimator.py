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
import time # Added for GQL retry delay
import asyncio # For ADO async logic
import base64 # For Azure DevOps PAT encoding
import re # For parsing Link header (though less used now)
from typing import Optional, Dict, Any, List, Tuple

# Attempt to import aiohttp for Azure DevOps async operations
try:
    import aiohttp
    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False
    aiohttp = None 
    logging.getLogger(__name__).warning(
        "Failed to import aiohttp. Azure DevOps labor estimation via async API calls will be skipped."
    )
# Attempt to import GraphQL clients
# Ensure Azure SDK components are potentially available for SPN
try:
    from azure.identity import ClientSecretCredential, CredentialUnavailableError
    AZURE_IDENTITY_AVAILABLE = True
except ImportError:
    AZURE_IDENTITY_AVAILABLE = False
    ClientSecretCredential = None # type: ignore
    CredentialUnavailableError = None # type: ignore
    if AIOHTTP_AVAILABLE: # Only warn if aiohttp is available, as ADO estimation depends on it
        logging.getLogger(__name__).warning(
            "azure-identity library not found. Azure DevOps labor estimation via SPN will not be available."
        )


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

# Placeholders for SPN details, matching those in azure_devops_connector.py
PLACEHOLDER_AZURE_TOKEN = "YOUR_AZURE_DEVOPS_PAT" # Define the missing constant
PLACEHOLDER_AZURE_CLIENT_ID = "YOUR_AZURE_CLIENT_ID"
PLACEHOLDER_AZURE_CLIENT_SECRET = "YOUR_AZURE_CLIENT_SECRET"
PLACEHOLDER_AZURE_TENANT_ID = "YOUR_AZURE_TENANT_ID"

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

def _get_azure_devops_auth_header_val(pat_token: str, logger_instance: logging.Logger) -> Optional[str]:
    """
    Encodes the Azure DevOps PAT for the Authorization header.
    Returns the header value string or None if encoding fails.
    """
    if not pat_token:
        logger_instance.error(f"{ANSI_RED}Azure DevOps PAT is missing.{ANSI_RESET}")
        return None
    try:
        # For PAT, the username part is empty, and the token is the password part.
        # The string to encode is ":<PAT>"
        pat_for_b64 = f":{pat_token}"
        pat_b64_bytes = base64.b64encode(pat_for_b64.encode('utf-8'))
        return f"Basic {pat_b64_bytes.decode('utf-8')}"
    except Exception as e:
        logger_instance.error(f"{ANSI_RED}Failed to encode Azure DevOps PAT: {e}{ANSI_RESET}", exc_info=True)
        return None

# --- Analysis Functions ---

def analyze_local_repo(
    repo_path: str, 
    hours_per_commit: Optional[float] = 0.5,
    logger_instance: Optional[logging.Logger] = None) -> pd.DataFrame:
    """
    Analyzes a local Git repository for commit history and estimates labor hours.
    """
    platform_identifier = f"Local Git repository: {repo_path}"
    current_logger = logger_instance if logger_instance else logger
    if hours_per_commit is None or hours_per_commit <= 0:
        current_logger.info(f"hours_per_commit is not set or invalid for {platform_identifier}. "
                      "Skipping labor hours estimation and returning empty DataFrame.")
        return _create_summary_dataframe([], 0.0)

    current_logger.info(f"Analyzing {platform_identifier} for labor hours.")
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
                current_logger.info(f"{platform_identifier} is empty or has no commits. No labor hours to estimate.")
            else:
                current_logger.error(f"Error running git log for {platform_identifier}: {error_message}")
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
                current_logger.warning(f"Unexpected git log line format for {platform_identifier}: {line}")
                
    except FileNotFoundError:
        current_logger.error(f"Git command not found. Please ensure Git is installed and in your PATH for {platform_identifier}.")
    except subprocess.CalledProcessError as e: # Should be caught by check=False and returncode check
        current_logger.error(f"Git log command failed for {platform_identifier}: {e.stderr}")
    except Exception as e:
        current_logger.error(f"An unexpected error occurred while analyzing {platform_identifier}: {e}", exc_info=True)

    return _create_summary_dataframe(processed_commits, hours_per_commit)


def analyze_github_repo_sync(
    owner: str, 
    repo: str, 
    token: str, 
    hours_per_commit: Optional[float] = 0.5, 
    github_api_url: str = "https://api.github.com", # Used for constructing GQL client endpoint
    logger_instance: Optional[logging.Logger] = None,
    default_branch_override: Optional[str] = None, # Added to accept specific default branch
    cfg_obj: Optional[Any] = None, 
    num_repos_in_target: Optional[int] = None,
    is_empty_repo: bool = False,
    number_of_workers: int = 1 # For logging/context if needed
) -> pd.DataFrame:
    """
    Analyzes a GitHub repository for commit history using GraphQL and estimates labor hours.
    """
    platform_identifier = f"GitHub repository: {owner}/{repo}"
    current_logger = logger_instance if logger_instance else logger


    if hours_per_commit is None or hours_per_commit <= 0:
        current_logger.info(f"hours_per_commit is not set or invalid for {platform_identifier}. "
                      "Skipping labor hours estimation and returning empty DataFrame.")
        return _create_summary_dataframe([], 0.0)
    if is_empty_repo:
        current_logger.info(f"{platform_identifier} is marked as empty. "
                      "Skipping labor hours estimation and returning empty DataFrame.")
        return _create_summary_dataframe([], 0.0)
    
    current_logger.info(f"Analyzing {platform_identifier} for labor hours using GraphQL.")

    if not GRAPHQL_CLIENTS_AVAILABLE or not github_gql:
        current_logger.error(f"GitHub GraphQL client not available for {platform_identifier}. Skipping labor estimation.")
        return _create_summary_dataframe([], 0.0)
    
    base_url_for_gql_client: Optional[str] = None
    if github_api_url and github_api_url.strip():
        if "api.github.com" not in github_api_url.lower(): # Indicates GHES
            base_url_for_gql_client = github_api_url.rstrip('/').replace('/api/v3', '').rstrip('/')
    gql_client: Optional[GqlClient] = None
    try:
        gql_client = github_gql.get_github_gql_client(token, base_url_for_gql_client)
    except Exception as e_client:
        current_logger.error(f"Failed to initialize GitHub GraphQL client for {platform_identifier}: {e_client}", exc_info=True)
        return _create_summary_dataframe([], 0.0)

    # --- GQL Fetch with Retry for Rate Limiting (similar to main connector) ---
    MAX_GQL_RETRIES = int(getattr(cfg_obj, 'GITHUB_GQL_MAX_RETRIES_ENV', os.getenv("GITHUB_GQL_MAX_RETRIES", "3"))) if cfg_obj else 3
    INITIAL_RETRY_DELAY = float(getattr(cfg_obj, 'GITHUB_GQL_INITIAL_RETRY_DELAY_ENV', os.getenv("GITHUB_GQL_INITIAL_RETRY_DELAY", "60"))) if cfg_obj else 60.0
    BACKOFF_FACTOR = float(getattr(cfg_obj, 'GITHUB_GQL_RETRY_BACKOFF_FACTOR_ENV', os.getenv("GITHUB_GQL_RETRY_BACKOFF_FACTOR", "2"))) if cfg_obj else 2.0
    MAX_INDIVIDUAL_DELAY = float(getattr(cfg_obj, 'GITHUB_GQL_MAX_INDIVIDUAL_RETRY_DELAY_ENV', os.getenv("GITHUB_GQL_MAX_INDIVIDUAL_RETRY_DELAY", "900"))) if cfg_obj else 900.0

    processed_commits: List[Tuple[str, str, datetime]] = []
    commit_data_tuples: List[Tuple[str, str, str]] = []

    if gql_client:
        for attempt in range(MAX_GQL_RETRIES + 1):
            try:
                if attempt > 0:
                    delay = INITIAL_RETRY_DELAY * (BACKOFF_FACTOR ** (attempt - 1))
                    delay = min(delay, MAX_INDIVIDUAL_DELAY)
                    current_logger.info(
                        f"LaborEst GQL Rate Limit Retry {attempt}/{MAX_GQL_RETRIES} for {platform_identifier}. "
                        f"Waiting {delay:.2f}s...",
                        extra={'org_group': platform_identifier}
                    )
                    time.sleep(delay)

                current_logger.debug(f"LaborEst GQL fetch_commit_history attempt {attempt + 1} for {platform_identifier}")
                commit_data_tuples = github_gql.fetch_commit_history_graphql(
                    client=gql_client,
                    owner=owner,
                    repo_name=repo,
                    default_branch_name=default_branch_override,
                    logger_instance=current_logger
                )
                current_logger.debug(f"LaborEst GQL fetch_commit_history attempt {attempt + 1} SUCCEEDED for {platform_identifier}")
                break # Success
            except github_gql.TransportQueryError as gql_err_retry: # Assuming github_gql.TransportQueryError exists
                is_rate_limited = False
                if gql_err_retry.errors and isinstance(gql_err_retry.errors, list):
                    for err_detail in gql_err_retry.errors:
                        if isinstance(err_detail, dict) and err_detail.get('type') == 'RATE_LIMITED':
                            is_rate_limited = True
                            break
                
                if is_rate_limited:
                    current_logger.warning(
                        f"LaborEst GQL RATE LIMIT detected on attempt {attempt + 1} for {platform_identifier}. "
                        f"Details: {gql_err_retry.errors}",
                        extra={'org_group': platform_identifier}
                    )
                    if attempt < MAX_GQL_RETRIES:
                        continue # Retry
                    else:
                        current_logger.error(
                            f"LaborEst GQL RATE LIMIT: Max retries ({MAX_GQL_RETRIES}) reached for {platform_identifier}. Skipping commit history.",
                            extra={'org_group': platform_identifier}
                        )
                        commit_data_tuples = [] # Ensure it's empty
                        break # Max retries reached
                else:
                    current_logger.error(f"LaborEst GQL TransportQueryError (not rate limit) for {platform_identifier}: {gql_err_retry.errors}. Skipping commit history.")
                    commit_data_tuples = []
                    break # Non-rate-limit GQL error
            except Exception as e_gql_hist:
                current_logger.error(f"LaborEst Unexpected error fetching GQL commit history for {platform_identifier}: {e_gql_hist}. Skipping commit history.", exc_info=True)
                commit_data_tuples = []
                break # Unexpected error

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
    gl_instance_for_gql: Optional['python_gitlab_library.Gitlab'] = None,  # Accept an existing client
    default_branch_override: Optional[str] = None,
    cfg_obj: Optional[Any] = None,
    num_repos_in_target: Optional[int] = None,
    is_empty_repo: bool = False,
    number_of_workers: int = 1, # For logging/context if needed
    logger_instance: Optional[logging.Logger] = None
) -> pd.DataFrame:
    """
    Analyzes a GitLab repository for commit history using GraphQL and estimates labor hours.
    """
    platform_identifier = f"GitLab project ID: {project_id}"
    current_logger = logger_instance if logger_instance else logger


    if hours_per_commit is None or hours_per_commit <= 0:
        current_logger.info(f"hours_per_commit is not set or invalid for {platform_identifier}. "
                      "Skipping labor hours estimation and returning empty DataFrame.")
        return _create_summary_dataframe([], 0.0)
    if is_empty_repo:
        current_logger.info(f"{platform_identifier} is marked as empty. "
                      "Skipping labor hours estimation and returning empty DataFrame.")
        return _create_summary_dataframe([], 0.0)

    current_logger.info(f"Analyzing {platform_identifier} for labor hours using GraphQL.")

    if not GRAPHQL_CLIENTS_AVAILABLE or not gitlab_gql or not python_gitlab_library:
        current_logger.error(f"GitLab GraphQL client or python-gitlab library not available for {platform_identifier}. Skipping labor estimation.")
        return _create_summary_dataframe([], 0.0)

    gl_instance_to_use = gl_instance_for_gql 
    if not gl_instance_to_use:
        current_logger.debug(f"No existing GitLab client passed for {platform_identifier}, creating a new one for labor estimation.")
        try:
            ssl_verify_flag = os.getenv("DISABLE_SSL_VERIFICATION", "false").lower() != "true"
            gl_instance_to_use = python_gitlab_library.Gitlab(gitlab_api_url.strip('/'), private_token=token, ssl_verify=ssl_verify_flag, timeout=30)
            gl_instance_to_use.auth() 
        except Exception as e_client:
            current_logger.error(f"Failed to initialize new GitLab client for {platform_identifier}: {e_client}", exc_info=True)
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
            current_logger.error(f"Failed to get project details (full_path, default_branch) for GitLab project ID {project_id}: {e_proj_get}")
            
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
    # PAT and SPN details are now optional
    pat_token: Optional[str] = None,
    spn_client_id: Optional[str] = None,
    spn_client_secret: Optional[str] = None,
    spn_tenant_id: Optional[str] = None,
    hours_per_commit: Optional[float] = 0.5,
    azure_devops_api_url: str = "https://dev.azure.com",
    cfg_obj: Optional[Any] = None,
    num_repos_in_target: Optional[int] = None,
    is_empty_repo: bool = False,
    number_of_workers: int = 1, # For logging/context if needed
    # logger_instance: Optional[logging.LoggerAdapter] = None # Corrected type hint
    logger_instance: Optional[logging.Logger] = None # Keep as Logger for broader compatibility

) -> pd.DataFrame:
    """
    Analyzes an Azure DevOps repository for commit history using REST API (aiohttp)
    and estimates labor hours.
    """
    platform_identifier = f"Azure DevOps repository: {organization}/{project}/{repo_id}"
    current_logger = logger_instance if logger_instance else logger

    if hours_per_commit is None or hours_per_commit <= 0:
        current_logger.info(f"hours_per_commit is not set or invalid for {platform_identifier}. "
                      "Skipping labor hours estimation and returning empty DataFrame.")
        return _create_summary_dataframe([], 0.0)
    if is_empty_repo:
        current_logger.info(f"{platform_identifier} is marked as empty. "
                      "Skipping labor hours estimation and returning empty DataFrame.")
        return _create_summary_dataframe([], 0.0)
    if not AIOHTTP_AVAILABLE:
        current_logger.error(f"aiohttp library not available. Skipping Azure DevOps labor estimation for {platform_identifier}.")
        return _create_summary_dataframe([], 0.0)

    current_logger.info(f"Analyzing {platform_identifier} for labor hours using API: {azure_devops_api_url}")

    top = 100 # ADO API items per page
    api_version = "6.0" 
    
    def _ado_process_item(commit_item: Dict[str, Any],
                          logger_instance: logging.Logger) -> Optional[Tuple[str, str, datetime]]:
        author_info = commit_item.get("author", {})
        author_name = author_info.get("name", "Unknown Author")
        author_email = author_info.get("email", "unknown@example.com")
        date_str = author_info.get("date") # This is usually committer date for ADO
        if not date_str: date_str = commit_item.get("committer", {}).get("date") # Fallback

        if date_str:
            commit_date = _parse_iso_datetime_str(date_str, logger_instance, platform_identifier, " for Azure DevOps commit")
            if commit_date:
                return (author_name, author_email, commit_date)
        return None

    auth_headers_for_ado = {}
    auth_method_used = "None"

    # Try SPN first if details are provided and valid
    if AZURE_IDENTITY_AVAILABLE and ClientSecretCredential and \
       spn_client_id and spn_client_secret and spn_tenant_id and \
       not (spn_client_id == PLACEHOLDER_AZURE_CLIENT_ID or \
            spn_client_secret == PLACEHOLDER_AZURE_CLIENT_SECRET or \
            spn_tenant_id == PLACEHOLDER_AZURE_TENANT_ID):
        try:
            current_logger.info(f"LaborEst: Attempting to get Azure AD token for SPN for {platform_identifier}.")
            credential = ClientSecretCredential(
                tenant_id=spn_tenant_id,
                client_id=spn_client_id,
                client_secret=spn_client_secret
            )
            access_token_obj = credential.get_token("499b84ac-1321-427f-aa17-267ca6975798/.default")
            auth_headers_for_ado["Authorization"] = f"Bearer {access_token_obj.token}"
            auth_method_used = "Service Principal"
            current_logger.info(f"LaborEst: Successfully obtained Azure AD token using SPN for {platform_identifier}.")
        except CredentialUnavailableError as e: # type: ignore
            current_logger.error(f"LaborEst: SPN CredentialUnavailableError for {platform_identifier}: {e}. Will try PAT if available.")
        except Exception as e:
            current_logger.error(f"LaborEst: Failed to get Azure AD token using SPN for {platform_identifier}: {e}. Will try PAT if available.", exc_info=True)
    
    # Fallback to PAT if SPN not used or failed
    if not auth_headers_for_ado.get("Authorization") and pat_token and pat_token != PLACEHOLDER_AZURE_TOKEN: # Ensure PLACEHOLDER_AZURE_TOKEN is defined or imported
        pat_auth_val = _get_azure_devops_auth_header_val(pat_token, current_logger)
        if pat_auth_val:
            auth_headers_for_ado["Authorization"] = pat_auth_val
            auth_method_used = "PAT"
            current_logger.info(f"LaborEst: Using PAT for Azure DevOps commit fetch for {platform_identifier}.")
    
    if not auth_headers_for_ado.get("Authorization"):
        current_logger.error(f"LaborEst: No valid authentication (SPN or PAT) configured for Azure DevOps {platform_identifier}. Skipping commit fetch.")
        return _create_summary_dataframe([], 0.0)

    async def fetch_ado_commits_async(logger_instance: logging.Logger):
        commits_data: List[Tuple[str, str, datetime]] = []
        _current_skip_async = 0 
        max_commits_to_fetch = 5000 # Safety limit
        fetched_count = 0

        try:
            from utils.api_delayer import apply_post_api_call_delay 
        except ImportError:
            logger_instance.error("Required import for ADO async fetch (api_delayer or aiohttp) missing.") # Should be caught by AIOHTTP_AVAILABLE
            return []

        async with aiohttp.ClientSession(headers=auth_headers_for_ado) as aio_session:
            while fetched_count < max_commits_to_fetch:
                current_url_to_fetch = (f"{azure_devops_api_url.rstrip('/')}/{organization}/{project}/_apis/git/repositories/{repo_id}"
                                       f"/commits?api-version={api_version}&$top={top}&$skip={_current_skip_async}") # type: ignore
                logger_instance.debug(f"Async fetching ADO commits (using {auth_method_used}) for {platform_identifier} from: {current_url_to_fetch}")
                try:
                    async with aio_session.get(current_url_to_fetch, timeout=aiohttp.ClientTimeout(total=30)) as response:
                        response.raise_for_status()
                        page_json = await response.json()
                        page_items = page_json.get("value", [])

                        if not page_items:
                            break 
                        
                        for item in page_items:
                            if fetched_count >= max_commits_to_fetch: break
                            processed = _ado_process_item(item, current_logger)
                            if processed:
                                commits_data.append(processed)
                                fetched_count += 1
                        
                        if len(page_items) < top or fetched_count >= max_commits_to_fetch:
                            break 
                        else:
                            _current_skip_async += top
                except aiohttp.ClientResponseError as e:
                    logger_instance.error(f"AIOHTTP ClientResponseError ({e.status}) for ADO {platform_identifier} from {current_url_to_fetch}: {e.message}")
                    if e.status == 401: # Unauthorized
                        logger_instance.error(f"LaborEst: ADO commit fetch failed with 401 Unauthorized using {auth_method_used}. Check token/SPN permissions for {platform_identifier}.")
                    break
                except Exception as e:
                    logger_instance.error(f"Error processing ADO commit page for {platform_identifier} from {current_url_to_fetch}: {e}", exc_info=True)
                    break
                finally:
                    if cfg_obj: 
                        await apply_post_api_call_delay("AZURE_DEVOPS", cfg_obj, num_repos_in_target)
        logger_instance.info(f"Fetched a total of {len(commits_data)} commit data entries for ADO repo {platform_identifier} via aiohttp (using {auth_method_used}).")
        return commits_data

    processed_commits: List[Tuple[str, str, datetime]] = []
    try:
        processed_commits = asyncio.run(fetch_ado_commits_async(current_logger))
    except RuntimeError as e:
        current_logger.error(f"RuntimeError running async ADO commit fetch for {platform_identifier}: {e}. This might be due to nested event loops.")
    except Exception as e_outer:
        current_logger.error(f"Outer exception running async ADO commit fetch for {platform_identifier}: {e_outer}", exc_info=True, extra={'org_group': organization })
            
    return _create_summary_dataframe(processed_commits, hours_per_commit)
