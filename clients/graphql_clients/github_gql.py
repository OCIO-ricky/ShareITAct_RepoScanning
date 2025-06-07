# clients/graphql_clients/github_gql.py
"""
GraphQL client for fetching repository data from GitHub with adaptive rate limit handling.
"""
from datetime import datetime, timezone # Added for rate limit reset time calculation
import logging
import time # Added for sleep functionality
from typing import Optional, Dict, Any, List, Tuple

from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
from gql.transport.exceptions import TransportQueryError
from utils.retry_utils import execute_with_retry # Import the new utility

logger = logging.getLogger(__name__)

GITHUB_GRAPHQL_ENDPOINT = "https://api.github.com/graphql" # Default, can be overridden for GHES

# Custom exception to carry rate limit reset information
class GithubGqlRateLimitError(TransportQueryError):
    def __init__(self, *args, errors: Optional[List[Dict[str, Any]]] = None, reset_at_iso: Optional[str] = None, **kwargs):
        # Ensure 'errors' is passed to the parent if provided
        super().__init__(*args, errors=errors, **kwargs)
        self.reset_at_iso: Optional[str] = reset_at_iso
        self.wait_seconds: Optional[float] = None
        if reset_at_iso:
            try:
                reset_dt = datetime.fromisoformat(reset_at_iso.replace('Z', '+00:00'))
                now_utc = datetime.now(timezone.utc)
                self.wait_seconds = max(0.0, (reset_dt - now_utc).total_seconds())
                logger.info(f"GithubGqlRateLimitError: Calculated wait_seconds: {self.wait_seconds:.2f}s from reset_at: {reset_at_iso}")
            except ValueError:
                logger.warning(f"Could not parse resetAt timestamp from GQL payload: {reset_at_iso}")

# Define common README and CODEOWNERS paths to try
COMMON_README_PATHS = ["README.md", "README.txt", "README", "readme.md"]
COMMON_CODEOWNERS_PATHS = ["CODEOWNERS", ".github/CODEOWNERS", "docs/CODEOWNERS"]

def get_github_gql_client(token: str, base_url: Optional[str] = None) -> Client:
    """Creates a GitHub GraphQL client."""
    endpoint: str
    if base_url and base_url.strip(): # If a base_url is provided (likely for GHES)
        # Ensure it's just the base (e.g., https://github.mycompany.com)
        # Remove common API suffixes if present
        cleaned_base_url = base_url.rstrip('/').replace('/api/v3', '').replace('/api/graphql', '').rstrip('/')
        endpoint = f"{cleaned_base_url}/api/graphql"
    else: # Default to public GitHub
        endpoint = GITHUB_GRAPHQL_ENDPOINT
    
    transport = RequestsHTTPTransport(
        url=endpoint,
        headers={"Authorization": f"Bearer {token}"},
        verify=True, # Consider making this configurable like in the REST connector
        retries=3,
    )
    return Client(transport=transport, fetch_schema_from_transport=False)

def build_file_queries(paths: List[str], actual_expression_prefix: str) -> str:
    """Builds parts of a GraphQL query to fetch multiple file contents."""
    query_parts = []
    for i, path in enumerate(paths):
        # Sanitize alias name: replace special characters with underscores
        alias_name = path.replace('.', '_').replace('/', '_')
        query_parts.append(f"""
        file_{alias_name}_{i}: object(expression: "{actual_expression_prefix}:{path}") {{
          ... on Blob {{
            text
            byteSize
          }}
        }}
        """)
    return "\n".join(query_parts)

COMPREHENSIVE_REPO_QUERY = gql(f"""
query GetRepositoryDetails(
    $owner: String!,
    $name: String!
) {{
  repository(owner: $owner, name: $name) {{
    id
    databaseId
    name
    nameWithOwner
    description
    url
    homepageUrl
    isFork
    isArchived
    isPrivate
    visibility # PUBLIC, PRIVATE, INTERNAL
    createdAt
    pushedAt
    updatedAt
    diskUsage # in kilobytes

    defaultBranchRef {{
      name
      target {{
        ... on Commit {{
          oid # This is the commit SHA
          history(first: 1) {{ # For last commit details if needed beyond just SHA
             nodes {{
                committedDate
             }}
          }}
        }}
      }}
    }}

    languages(first: 10, orderBy: {{field: SIZE, direction: DESC}}) {{
      edges {{
        node {{
          name
        }}
      }}
    }}

    repositoryTopics(first: 20) {{
      nodes {{
        topic {{
          name
        }}
      }}
    }}

    licenseInfo {{
      spdxId
      name
      url
    }}

    # README files
    {build_file_queries(COMMON_README_PATHS, "HEAD")}

    # CODEOWNERS files
    {build_file_queries(COMMON_CODEOWNERS_PATHS, "HEAD")}

    tags: refs(refPrefix: "refs/tags/", first: 100, orderBy: {{field: TAG_COMMIT_DATE, direction: DESC}}) {{
      nodes {{
        name
      }}
    }}
  }} # Closing the repository block
  rateLimit {{ # Query rateLimit as a top-level field
    limit
    remaining
    resetAt
  }}
}} # Closing the query block
""")

def _is_gql_rate_limited_error(query_error: TransportQueryError) -> bool:
    """
    Checks if a TransportQueryError from the GQL client is due to a GitHub rate limit.
    """
    if not isinstance(query_error, TransportQueryError):
        return False
    if query_error.errors and isinstance(query_error.errors, list):
        for error_detail in query_error.errors:
            if isinstance(error_detail, dict) and error_detail.get('type') == 'RATE_LIMITED':
                return True
    return False

def _get_github_gql_retry_wait_seconds(e: Exception) -> Optional[float]:
    """Extracts wait_seconds from our custom GithubGqlRateLimitError."""
    if isinstance(e, GithubGqlRateLimitError):
        return e.wait_seconds
    return None

def fetch_repository_details_graphql(
    client: Client,
    owner: str,
    repo_name: str,
    logger_instance: Optional[logging.Logger] = None, # Renamed for consistency
    # Retry parameters for rate limiting
    max_retries: int = 3,
    initial_delay_seconds: float = 60.0,
    backoff_factor: float = 2.0,
    max_individual_delay_seconds: float = 900.0
) -> Optional[Dict[str, Any]]:
    """Fetches comprehensive repository details using GraphQL."""
    params = {
        "owner": owner,
        "name": repo_name,
    }
    current_logger = logger_instance if logger_instance else logger # Use consistent naming

    def _api_call():
        current_logger.debug(f"Executing GraphQL query for {owner}/{repo_name}")
        # COMPREHENSIVE_REPO_QUERY now also fetches rateLimit
        result = client.execute(COMPREHENSIVE_REPO_QUERY, variable_values=params)
        
        errors = result.get("errors")
        if errors:
            current_logger.error(f"GraphQL query for {owner}/{repo_name} returned errors: {errors}")
            is_rl = any(err.get('type') == 'RATE_LIMITED' for err in errors if isinstance(err, dict))
            if is_rl:
                # rateLimit is now a top-level field in the result
                reset_at = safe_get(result, "rateLimit", "resetAt") 
                raise GithubGqlRateLimitError(errors=errors, reset_at_iso=reset_at, message=str(errors))
            raise TransportQueryError(errors=errors, message=str(errors)) # Standard GQL error

        current_logger.debug(f"Successfully fetched GraphQL data for {owner}/{repo_name}")
        return result.get("repository")
    return execute_with_retry(
        api_call_func=_api_call,
        is_rate_limit_error_func=lambda e: isinstance(e, GithubGqlRateLimitError),
        get_retry_after_seconds_func=_get_github_gql_retry_wait_seconds,
        max_retries=max_retries,
        initial_delay_seconds=initial_delay_seconds,
        backoff_factor=backoff_factor,
        max_individual_delay_seconds=max_individual_delay_seconds,
        error_logger=current_logger,
        log_context=f"GraphQL comprehensive details for {owner}/{repo_name}"
    )

def safe_get(d, *keys):
    for key in keys:
        if isinstance(d, dict):
            d = d.get(key)
        else:
            return None
    return d
  
def fetch_repository_short_metadata_graphql(
    client: Client,
    owner: str,
    repo_name: str,
    logger_instance: logging.Logger,
    # Retry parameters (can be tuned for "peek" calls)
    max_retries: int = 2,
    initial_delay_seconds: float = 30.0,
    backoff_factor: float = 1.5,
    max_individual_delay_seconds: float = 300.0
) -> Optional[Dict[str, Any]]:
    """
    Fetches minimal repository metadata (ID, last commit SHA, isEmpty) using GraphQL.
    Used for peek-ahead cache checks.
    """
    query_str = """
      query RepoShortMetadata($owner: String!, $repoName: String!) {
        repository(owner: $owner, name: $repoName) {
          id # GraphQL Node ID
          databaseId # Integer ID, good for cache key
          nameWithOwner
          isEmpty
          pushedAt # Add pushedAt to get the last push date
          defaultBranchRef {
            name
            target {
              ... on Commit {
                oid # This is the commit SHA
              }
            }
          }
        }
        rateLimit { # Fetch rateLimit as a top-level field
          limit
          remaining
          resetAt
        }
      }
    """
    query = gql(query_str)
    variables = {"owner": owner, "repoName": repo_name}
    org_group_context = f"{owner}/{repo_name}"

    def _api_call():
        logger_instance.debug(f"GQL Peek: Executing short metadata query for {owner}/{repo_name}", extra={'org_group': org_group_context})
        result = client.execute(query, variable_values=variables)

        errors = result.get("errors")
        if errors:
            logger_instance.error(f"GQL Peek: GraphQL query for {owner}/{repo_name} returned errors: {errors}", extra={'org_group': org_group_context})
            is_rl = any(err.get('type') == 'RATE_LIMITED' for err in errors if isinstance(err, dict))
            if is_rl:
                # The rateLimit data will now be at the top level of the result, not under 'repository'
                reset_at = safe_get(result, "rateLimit", "resetAt")
                raise GithubGqlRateLimitError(errors=errors, reset_at_iso=reset_at, message=str(errors))
            raise TransportQueryError(errors=errors, message=str(errors))

        # Repository data is still under result.get("repository")
        if result and result.get("repository"):
            repo_info = result.get("repository")
            commit_sha = safe_get(repo_info, "defaultBranchRef", "target", "oid")
            logger_instance.debug(f"GQL Peek: Successfully fetched short metadata for {owner}/{repo_name}", extra={'org_group': org_group_context})
            return {
                "id": repo_info.get("databaseId"),  # Integer ID
                "lastCommitSHA": commit_sha,
                "isEmpty": repo_info.get("isEmpty", False),
                "pushedAt": repo_info.get("pushedAt"), # Return pushedAt
                # Optionally, you can also return the rateLimit info from the result if needed by the caller
                # "rateLimit": result.get("rateLimit")
            }
        logger_instance.warning(f"GQL Peek: No repository data returned for {owner}/{repo_name}. Result: {result}", extra={'org_group': org_group_context})
        # If repository is not found, GQL might return data: { repository: null }.
        # This is not an exception, so handle it post-call.
        return {"id": None, "lastCommitSHA": None, "isEmpty": True, "pushedAt": None} # Default for not found

    try:
        return execute_with_retry(
            api_call_func=_api_call,
            is_rate_limit_error_func=lambda e: isinstance(e, GithubGqlRateLimitError),
            get_retry_after_seconds_func=_get_github_gql_retry_wait_seconds,
            max_retries=max_retries,
            initial_delay_seconds=initial_delay_seconds,
            backoff_factor=backoff_factor,
            max_individual_delay_seconds=max_individual_delay_seconds,
            error_logger=logger_instance,
            log_context=f"GraphQL short metadata for {owner}/{repo_name}"
        )
    except Exception as e: # Catch errors re-raised by execute_with_retry or other unexpected ones
        logger_instance.error(f"GQL Peek: Failed to fetch short metadata for {owner}/{repo_name} after retries or due to non-retryable error: {e}", exc_info=True, extra={'org_group': org_group_context})
        return {"id": None, "lastCommitSHA": None, "isEmpty": True, "pushedAt": None, "error": str(e)}

COMMIT_HISTORY_QUERY = gql("""
query GetCommitHistory($owner: String!, $name: String!, $branch: String, $afterCursor: String) {
  repository(owner: $owner, name: $name) {
    object(expression: $branch) {
      ... on Commit {
        history(first: 100, after: $afterCursor) {
          pageInfo {
            endCursor
            hasNextPage
          }
          nodes {
            author {
              name
              email
              date # This is the commit author date
            }
            # oid # Commit SHA if needed
            # committedDate # If you need committer date instead of author date
          }
        }
      }
    }
  }
  rateLimit { # Fetch rateLimit as a top-level field
    limit
    remaining
    resetAt
  }
}
""")

def fetch_commit_history_graphql(
    client: Client,
    owner: str,
    repo_name: str,
    default_branch_name: Optional[str] = None,
    logger_instance: Optional[logging.Logger] = None, # Accept a logger instance
    max_commits_to_fetch_for_labor: int = 5000, # Safety limit
    # Retry parameters for each page fetch
    max_page_retries: int = 2,
    initial_page_delay_seconds: float = 20.0,
    page_backoff_factor: float = 1.5,
    max_individual_page_delay_seconds: float = 180.0
) -> List[Tuple[str, str, str]]: # (author_name, author_email, commit_date_iso_string)
    """
    Fetches commit history for a repository using GraphQL, handling pagination.
    """
    all_commits_data: List[Tuple[str, str, str]] = []
    current_cursor: Optional[str] = None
    has_next_page = True
    fetched_count = 0

    # If default_branch_name is None, the GQL query's $branch variable will be null.
    # GitHub's GraphQL API typically defaults to the repository's default branch
    # when the branch/expression for object() is not specified or is null.
    # So, passing None for default_branch_name should work as intended.
    branch_to_query = default_branch_name # This can be None
    current_logger = logger_instance if logger_instance else logger

    current_logger.info(f"Fetching commit history for {owner}/{repo_name} (branch: {branch_to_query or 'default'}) via GraphQL.")

    page_num = 0
    while has_next_page and fetched_count < max_commits_to_fetch_for_labor:
        page_num += 1
        final_params = {
            "owner": owner, "name": repo_name, "branch": branch_to_query, "afterCursor": current_cursor
        }

        def _fetch_page():
            current_logger.debug(f"Fetching commit history page {page_num} for {owner}/{repo_name}, cursor: {current_cursor}")
            page_result = client.execute(COMMIT_HISTORY_QUERY, variable_values=final_params)
            
            errors = page_result.get("errors")
            if errors:
                current_logger.error(f"GraphQL commit history page {page_num} for {owner}/{repo_name} returned errors: {errors}")
                is_rl = any(err.get('type') == 'RATE_LIMITED' for err in errors if isinstance(err, dict))
                if is_rl:
                    reset_at = safe_get(page_result, "rateLimit", "resetAt")
                    raise GithubGqlRateLimitError(errors=errors, reset_at_iso=reset_at, message=str(errors))
                raise TransportQueryError(errors=errors, message=str(errors))
            return page_result

        try:
            result = execute_with_retry(
                api_call_func=_fetch_page,
                is_rate_limit_error_func=lambda e: isinstance(e, GithubGqlRateLimitError),
                get_retry_after_seconds_func=_get_github_gql_retry_wait_seconds,
                max_retries=max_page_retries,
                initial_delay_seconds=initial_page_delay_seconds,
                backoff_factor=page_backoff_factor,
                max_individual_delay_seconds=max_individual_page_delay_seconds,
                error_logger=current_logger,
                log_context=f"GraphQL commit history page {page_num} for {owner}/{repo_name}"
            )

            repository_data = result.get("repository")
            if not repository_data:
                current_logger.warning(f"Repository not found or no data returned for {owner}/{repo_name} during commit history fetch.")
                break

            repo_object = repository_data.get("object")
            if not repo_object or "history" not in repo_object:
                current_logger.warning(f"No commit history found or branch '{branch_to_query or 'default'}' not found for {owner}/{repo_name}.")
                break

            history = repo_object["history"]
            for node in history.get("nodes", []):
                author = node.get("author")
                if author and author.get("name") and author.get("email") and author.get("date"):
                    # Ensure name and email are not None before appending
                    author_name = author["name"] if author["name"] is not None else "Unknown Author"
                    author_email = author["email"] if author["email"] is not None else "unknown@example.com"
                    all_commits_data.append((author_name, author_email, author["date"]))
                    fetched_count += 1
            
            page_info = history.get("pageInfo", {})
            has_next_page = page_info.get("hasNextPage", False)
            current_cursor = page_info.get("endCursor")
            current_logger.debug(f"Fetched page of {len(history.get('nodes', []))} commits for {owner}/{repo_name}. Total so far: {fetched_count}. HasNextPage: {has_next_page}")
        except (TransportQueryError, GithubGqlRateLimitError) as tqe: # Catch our custom error too
            current_logger.error(f"GraphQL query failed for commit history page {page_num} of {owner}/{repo_name}: {tqe.errors}")
            break # Stop fetching on error for this repo
        except Exception as e: # Other unexpected errors
            break # Stop fetching on error
        except Exception as e:
            current_logger.error(f"Unexpected error fetching commit history for {owner}/{repo_name}: {e}", exc_info=True)
            break
    
    current_logger.info(f"Fetched a total of {len(all_commits_data)} commit data entries for {owner}/{repo_name} via GraphQL.")
    return all_commits_data

# New query for rate limit status
RATE_LIMIT_QUERY = gql("""
query GetRateLimit {
  rateLimit {
    limit
    cost
    remaining
    resetAt # ISO8601 string timestamp for when the limit resets
  }
}
""")

def fetch_rate_limit_status_graphql(
    client: Client,
    logger_instance: Optional[logging.Logger] = None,
    # Parameters for sleep suggestion logic
    low_remaining_threshold_absolute: int = 50, # If remaining points < this, suggest sleeping till reset
    low_remaining_threshold_percentage: float = 0.05, # If remaining points < this % of limit, suggest sleeping till reset
    min_remaining_for_dynamic_calc: int = 10, # Min remaining points to attempt dynamic sleep calculation
    target_buffer_percentage: float = 0.10, # Aim to leave this % of points as a buffer when calculating dynamic sleep
    max_suggested_dynamic_sleep_seconds: float = 60.0, # Max cap for dynamically suggested sleep
    min_suggested_dynamic_sleep_seconds: float = 1.0, # Min cap for dynamically suggested sleep if not 0
    # Retry parameters for the rate limit query itself (should be very minimal)
    max_retries_rlq: int = 1,
    initial_delay_rlq_seconds: float = 5.0,
    backoff_factor_rlq: float = 1.0,
    max_individual_delay_rlq_seconds: float = 10.0
) -> Optional[Dict[str, Any]]:
    """
    Fetches the current GitHub GraphQL API rate limit status.
    This query itself typically costs 0 or 1 point against the rate limit.

    Returns:
        A dictionary containing:
          - limit (int): Max points per window.
          - cost (int): Cost of this rateLimit query.
          - remaining (int): Points remaining.
          - resetAt (str): ISO8601 timestamp for when the limit resets.
          - seconds_until_reset (float): Calculated seconds until the window resets.
          - suggested_sleep_seconds (float): Calculated suggestion for sleep time.
        Returns None if the query fails or data is not found.
    """
    current_logger = logger_instance if logger_instance else logger
    current_logger.debug("Executing GraphQL query for rate limit status.")

    def _api_call_rate_limit():
        return client.execute(RATE_LIMIT_QUERY)

    try:
        result = execute_with_retry(
            api_call_func=_api_call_rate_limit,
            is_rate_limit_error_func=lambda e: isinstance(e, GithubGqlRateLimitError), # Should ideally not hit RL here, but good practice
            get_retry_after_seconds_func=_get_github_gql_retry_wait_seconds,
            max_retries=max_retries_rlq,
            initial_delay_seconds=initial_delay_rlq_seconds,
            backoff_factor=backoff_factor_rlq,
            max_individual_delay_seconds=max_individual_delay_rlq_seconds,
            error_logger=current_logger,
            log_context="GraphQL rate limit status query"
        )
        rate_limit_data = result.get("rateLimit")
        if rate_limit_data:
            # Calculate suggested sleep time
            remaining_points = rate_limit_data.get("remaining", 0)
            limit_points = rate_limit_data.get("limit", 5000) # Default to 5000 if not present
            reset_at_str = rate_limit_data.get("resetAt")
            
            seconds_until_reset = 0.0
            suggested_sleep_seconds = 0.0

            if reset_at_str:
                try:
                    reset_dt = datetime.fromisoformat(reset_at_str.replace('Z', '+00:00'))
                    now_utc = datetime.now(timezone.utc)
                    seconds_until_reset = max(0.0, (reset_dt - now_utc).total_seconds())
                except ValueError:
                    current_logger.warning(f"Could not parse rate limit resetAt timestamp: {reset_at_str}")

            # Determine if we should sleep until reset
            threshold_abs_check = remaining_points < low_remaining_threshold_absolute
            threshold_pct_check = (remaining_points / limit_points) < low_remaining_threshold_percentage if limit_points > 0 else True

            if seconds_until_reset > 0 and (threshold_abs_check or threshold_pct_check):
                suggested_sleep_seconds = seconds_until_reset
                current_logger.info(f"Rate limit very low ({remaining_points}/{limit_points}). Suggesting sleep until reset: {suggested_sleep_seconds:.2f}s.")
            elif seconds_until_reset > 0 and remaining_points > min_remaining_for_dynamic_calc:
                # Dynamically calculate sleep time to spread out remaining requests, leaving a buffer
                usable_remaining = remaining_points - (limit_points * target_buffer_percentage)
                if usable_remaining > 0:
                    avg_time_per_request = seconds_until_reset / usable_remaining
                    suggested_sleep_seconds = min(avg_time_per_request, max_suggested_dynamic_sleep_seconds)
                    suggested_sleep_seconds = max(min_suggested_dynamic_sleep_seconds, suggested_sleep_seconds) # Ensure at least min if not 0
                else: # Not enough remaining to safely calculate, suggest max dynamic or wait
                    suggested_sleep_seconds = max_suggested_dynamic_sleep_seconds
                current_logger.info(f"Rate limit moderate ({remaining_points}/{limit_points}). Dynamically suggesting sleep: {suggested_sleep_seconds:.2f}s.")
            else: # Ample requests or reset time passed/unknown
                suggested_sleep_seconds = 0.0

            rate_limit_data["seconds_until_reset"] = round(seconds_until_reset, 2)
            rate_limit_data["suggested_sleep_seconds"] = round(suggested_sleep_seconds, 2)

            current_logger.info(
                f"GraphQL Rate Limit Status: Remaining {rate_limit_data.get('remaining')}/{rate_limit_data.get('limit')}. "
                f"Resets in: {rate_limit_data['seconds_until_reset']:.2f}s. "
                f"Suggested sleep: {rate_limit_data['suggested_sleep_seconds']:.2f}s. "
                f"Last query cost: {rate_limit_data.get('cost')}."
            )
            return rate_limit_data
        current_logger.warning("Rate limit data not found in GraphQL response when querying for status.")
        return None
    except TransportQueryError as tqe_rl: # Non-rate-limit GQL error, or rate limit after retries for this specific query
        current_logger.error(f"GraphQL query for rate limit status failed: {tqe_rl.errors}")
        return None # Propagate failure as None
    except Exception as e_unexp_rl:
        current_logger.error(f"Unexpected error during GraphQL rate limit status fetch: {e_unexp_rl}", exc_info=True)
        return None # Propagate failure as None
