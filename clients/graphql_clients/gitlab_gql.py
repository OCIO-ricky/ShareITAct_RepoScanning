# clients/graphql_clients/gitlab_gql.py
"""
GraphQL client for fetching repository data from GitLab.
Uses the python-gitlab library's built-in GraphQL support.
"""
import logging
import time # For potential sleep, though execute_with_retry handles it
from typing import Optional, Dict, Any, List, Tuple

# Import gitlab and its exceptions directly
import gitlab
from gitlab.exceptions import GitlabHttpError, GitlabError

from utils.retry_utils import execute_with_retry

logger = logging.getLogger(__name__)

# Define common README and CODEOWNERS paths to try
COMMON_README_PATHS_GITLAB = ["README.md", "README.txt", "README", "readme.md"]
COMMON_CODEOWNERS_PATHS_GITLAB = ["CODEOWNERS", ".gitlab/CODEOWNERS", "docs/CODEOWNERS"]

def build_gitlab_blob_queries(paths: List[str], alias_prefix: str) -> str:
    """Builds parts of a GitLab GraphQL query to fetch multiple file blobs."""
    query_parts = []
    for i, path in enumerate(paths):
        alias_name = path.replace('.', '_').replace('/', '_')
        query_parts.append(f"""
        {alias_prefix}_{alias_name}_{i}: blobs(paths: ["{path}"]) {{
          nodes {{
            rawTextBlob
            webPath
          }}
        }}
        """)
    return "\n".join(query_parts)


COMPREHENSIVE_PROJECT_QUERY_TEMPLATE = """
query GetProjectDetails($fullPath: ID!) {{
  project(fullPath: $fullPath) {{
    id # GraphQL Global ID, e.g., "gid://gitlab/Project/123"
    name
    fullPath
    description
    webUrl
    httpUrlToRepo
    archived
    visibility # public, private, internal
    createdAt
    lastActivityAt
    repository {{
      empty
      rootRef # Default branch name
      tree(ref: "{default_branch_placeholder}", recursive: false) {{
        lastCommit {{
          sha
        }}
      }}
      {readme_blobs_placeholder}
      {codeowners_blobs_placeholder}
    }}
    languages {{
      name
      share
    }}
    topics
    releases(first: 100) {{
      nodes {{
        tagName
      }}
    }}
  }}
}}
"""
def _is_gitlab_rate_limited_error(e: Exception) -> bool:
    """Checks if a GitlabHttpError is due to rate limiting (HTTP 429)."""
    if isinstance(e, GitlabHttpError):
        if e.response_code == 429:
            return True
    return False

def _get_gitlab_retry_after_seconds(e: Exception) -> Optional[float]:
    """Extracts Retry-After seconds from GitlabHttpError if present."""
    if isinstance(e, GitlabHttpError) and e.response_code == 429:
        if hasattr(e, 'response_headers'):
            retry_after_header = e.response_headers.get('Retry-After')
            if retry_after_header:
                try:
                    return float(retry_after_header)
                except ValueError:
                    logger.warning(f"Could not parse GitLab Retry-After header value: {retry_after_header}")
    return None

def fetch_project_details_graphql(
    gl_instance: gitlab.Gitlab,
    project_full_path: str,
    default_branch: Optional[str] = "main",
    logger_instance: Optional[logging.Logger] = None,
    max_retries: int = 3,
    initial_delay_seconds: float = 60.0,
    backoff_factor: float = 2.0,
    max_individual_delay_seconds: float = 900.0
) -> Optional[Dict[str, Any]]:
    """Fetches comprehensive project details using GitLab GraphQL."""
    current_logger = logger_instance if logger_instance else logger

    readme_blob_queries = build_gitlab_blob_queries(COMMON_README_PATHS_GITLAB, "readme")
    codeowners_blob_queries = build_gitlab_blob_queries(COMMON_CODEOWNERS_PATHS_GITLAB, "codeowners")
    
    query_string = COMPREHENSIVE_PROJECT_QUERY_TEMPLATE.format(
        default_branch_placeholder=default_branch or "main",
        readme_blobs_placeholder=readme_blob_queries,
        codeowners_blobs_placeholder=codeowners_blob_queries
    )
    params = {"fullPath": project_full_path}

    def _api_call():
        current_logger.debug(f"Executing GitLab GraphQL query for {project_full_path}")
        if not hasattr(gitlab, 'GraphQL'):
            current_logger.error("gitlab.GraphQL class is not available. Ensure python-gitlab version is >= 2.0.0.")
            raise ImportError("gitlab.GraphQL class not available.")

        gql_client = gitlab.GraphQL(url=gl_instance.url, token=gl_instance.private_token, ssl_verify=gl_instance.ssl_verify)
        result = gql_client.execute(query_string, variable_values=params)

        if "errors" in result and result["errors"]:
            current_logger.error(f"GitLab GraphQL query for {project_full_path} returned errors: {result['errors']}")
            raise GitlabError(f"GraphQL errors: {result['errors']}")
        
        current_logger.debug(f"Successfully fetched GitLab GraphQL data for {project_full_path}")
        return result.get("project")

    return execute_with_retry(
        api_call_func=_api_call,
        is_rate_limit_error_func=_is_gitlab_rate_limited_error,
        get_retry_after_seconds_func=_get_gitlab_retry_after_seconds,
        max_retries=max_retries,
        initial_delay_seconds=initial_delay_seconds,
        backoff_factor=backoff_factor,
        max_individual_delay_seconds=max_individual_delay_seconds,
        error_logger=current_logger,
        log_context=f"GitLab GraphQL project details for {project_full_path}"
    )

def fetch_project_short_metadata_graphql(
    gl_instance: gitlab.Gitlab,
    project_full_path: str,
    logger_instance: logging.Logger,
    max_retries: int = 2,
    initial_delay_seconds: float = 30.0,
    backoff_factor: float = 1.5,
    max_individual_delay_seconds: float = 300.0
) -> Optional[Dict[str, Any]]:
    """
    Fetches minimal project metadata (ID, last commit SHA, isEmpty) using GitLab GraphQL.
    Returns the integer project ID as 'id' if parsable from the GID.
    """
    current_logger = logger_instance
    org_group_context = project_full_path

    query_str = """
        query ProjectShortMetadata($fullPath: ID!) {
          project(fullPath: $fullPath) {
            id # GraphQL GID, e.g., "gid://gitlab/Project/123"
            fullPath
            repository {
              rootRef # Default branch name
              tree { # Querying tree on rootRef to get its lastCommit
                lastCommit {
                  sha
                }
              }
            }
          }
        }
    """
    variables = {"fullPath": project_full_path}

    def _api_call():
        current_logger.debug(
            f"GQL Peek: Fetching short metadata for GitLab project {project_full_path}",
            extra={'org_group': org_group_context}
        )
        if not hasattr(gitlab, 'GraphQL'):
            current_logger.error("gitlab.GraphQL class is not available. Ensure python-gitlab version is >= 2.0.0.")
            raise ImportError("gitlab.GraphQL class not available.")

        gql_client = gitlab.GraphQL(
            url=gl_instance.url,
            token=gl_instance.private_token,
            ssl_verify=gl_instance.ssl_verify,
        )
        result = gql_client.execute(query_str, variable_values=variables)

        if "errors" in result and result["errors"]:
            current_logger.error(f"GQL Peek: GitLab GraphQL query for {project_full_path} returned errors: {result['errors']}")
            raise GitlabError(f"GraphQL errors: {result['errors']}")

        if result and result.get("project"):
            project_info = result["project"]
            repo_info = project_info.get("repository", {})
            is_empty = not repo_info.get("rootRef")
            commit_sha = repo_info.get("tree", {}).get("lastCommit", {}).get("sha")
            gid = project_info.get("id")
            integer_id_str = gid.split('/')[-1] if gid and gid.startswith("gid://gitlab/Project/") else None
            
            current_logger.debug(
                f"GQL Peek: Successfully fetched short metadata for GitLab {project_full_path}",
                extra={'org_group': org_group_context}
            )
            return {
                "id": int(integer_id_str) if integer_id_str and integer_id_str.isdigit() else None,
                "lastCommitSHA": commit_sha,
                "isEmpty": is_empty,
            }
        
        current_logger.warning(
            f"GQL Peek: No project data returned for GitLab {project_full_path}. Result: {result}",
            extra={'org_group': org_group_context}
        )
        return {"id": None, "lastCommitSHA": None, "isEmpty": True, "error": "Project data not found in GQL response"}

    try:
        return execute_with_retry(
            api_call_func=_api_call,
            is_rate_limit_error_func=_is_gitlab_rate_limited_error,
            get_retry_after_seconds_func=_get_gitlab_retry_after_seconds,
            max_retries=max_retries,
            initial_delay_seconds=initial_delay_seconds,
            backoff_factor=backoff_factor,
            max_individual_delay_seconds=max_individual_delay_seconds,
            error_logger=current_logger,
            log_context=f"GitLab GraphQL short metadata for {project_full_path}"
        )
    except Exception as e:
        current_logger.error(
            f"GQL Peek: Failed to fetch short metadata for GitLab {project_full_path} after retries or due to non-retryable error: {e}",
            exc_info=True, extra={'org_group': org_group_context}
        )
        return {"id": None, "lastCommitSHA": None, "isEmpty": True, "error": str(e)}


def fetch_commit_history_graphql(
    gl_instance: gitlab.Gitlab,
    project_full_path: str,
    default_branch_name: Optional[str] = None,
    max_commits_to_fetch_for_labor: int = 5000,
    logger_instance: Optional[logging.Logger] = None
) -> List[Tuple[str, str, str]]:
    """
    Fetches commit history for a GitLab project using REST API, handling pagination.
    (Note: This implementation uses REST as per the provided context for this file.)
    """
    all_commits_data: List[Tuple[str, str, str]] = []
    fetched_count = 0
    page_num = 1
    commits_per_page = 100
    current_logger = logger_instance if logger_instance else logger

    current_logger.info(f"Fetching commit history for project {project_full_path} (branch: {default_branch_name or 'default'}) via REST API.")

    try:
        project = gl_instance.projects.get(project_full_path, lazy=True)
        
        while fetched_count < max_commits_to_fetch_for_labor:
            commits_page = project.commits.list(
                ref_name=default_branch_name,
                page=page_num,
                per_page=commits_per_page,
                all=False
            )

            if not commits_page:
                if page_num == 1:
                    current_logger.warning(f"No commit history found or branch '{default_branch_name or 'default'}' not found via REST API for {project_full_path}.")
                break

            for commit_obj in commits_page:
                if fetched_count >= max_commits_to_fetch_for_labor:
                    break
                
                author_name = commit_obj.author_name if commit_obj.author_name is not None else "Unknown Author"
                author_email = commit_obj.author_email if commit_obj.author_email is not None else "unknown@example.com"
                authored_date = commit_obj.authored_date
                
                if author_name and author_email and authored_date:
                    all_commits_data.append((author_name, author_email, authored_date))
                    fetched_count += 1
            
            current_logger.debug(f"Fetched page {page_num} of {len(commits_page)} commits for {project_full_path}. Total so far: {fetched_count}.")

            if len(commits_page) < commits_per_page or fetched_count >= max_commits_to_fetch_for_labor:
                break
            
            page_num += 1

    except gitlab.exceptions.GitlabGetError as e_get:
        current_logger.error(f"REST API error getting project {project_full_path} for commit history: {e_get}")
    except gitlab.exceptions.GitlabListError as e_list:
        current_logger.error(f"REST API error listing commits for {project_full_path}: {e_list}")
    except GitlabHttpError as e_http:
        current_logger.error(
            f"REST API HTTP error for commit history of {project_full_path}: "
            f"Status Code: {e_http.response_code}, Message: {getattr(e_http, 'error_message', str(e_http))}"
        )
    except GitlabError as e_gitlab:
        current_logger.error(
            f"General GitLab API error fetching commit history for {project_full_path}: {e_gitlab}"
        )
    except Exception as e:
        current_logger.error(f"Unexpected error fetching commit history for {project_full_path}: {e}", exc_info=True)
            
    current_logger.info(f"Fetched a total of {len(all_commits_data)} commit data entries for {project_full_path} via REST API.")
    return all_commits_data
