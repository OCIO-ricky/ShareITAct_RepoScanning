# clients/graphql_clients/gitlab_gql.py
"""
GraphQL client for fetching repository data from GitLab.
Uses the python-gitlab library's built-in GraphQL support.
"""
import logging
from typing import Optional, Dict, Any, List, Tuple
import gitlab # python-gitlab library

logger = logging.getLogger(__name__)

# Define common README and CODEOWNERS paths to try
COMMON_README_PATHS_GITLAB = ["README.md", "README.txt", "README", "readme.md"]
COMMON_CODEOWNERS_PATHS_GITLAB = ["CODEOWNERS", ".gitlab/CODEOWNERS", "docs/CODEOWNERS"]

def build_gitlab_blob_queries(paths: List[str], alias_prefix: str) -> str:
    """Builds parts of a GitLab GraphQL query to fetch multiple file blobs."""
    query_parts = []
    for i, path in enumerate(paths):
        alias_name = path.replace('.', '_').replace('/', '_')
        # Use blobs(paths: [String!]) which returns a list of Blob objects.
        # We query for one path at a time, so paths will be a list with one item.
        query_parts.append(f"""
        {alias_prefix}_{alias_name}_{i}: blobs(paths: ["{path}"]) {{ # blobs returns a BlobConnection
          nodes {{ # Access individual Blob objects through nodes
            rawTextBlob # Corrected field name as per GitLab's schema
            webPath # for constructing URL
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
    httpUrlToRepo # REST API clone URL
    archived
    visibility # public, private, internal
    createdAt
    lastActivityAt
    repository {{
      empty
      rootRef # Default branch name
      tree(ref: "{default_branch_placeholder}", recursive: false) {{ # Get commit SHA of default branch
        lastCommit {{
          sha
        }}
      }}
      # README files - These are fields of Repository, not Tree
      {readme_blobs_placeholder}
      # CODEOWNERS files - These are fields of Repository, not Tree
      {codeowners_blobs_placeholder}
    }}
    languages {{ # Keep languages via GQL
      name
      share
    }}
    topics # These are 'tags' in GitLab UI for projects
    # Git Tags (releases) - orderBy is NOT supported here
    releases(first: 100) {{
      nodes {{
        tagName
      }}
    }}
    # Add more fields if needed for labor hours estimation
    # statistics {{ commitCount }} # if needed and available
  }}
}}
"""

def fetch_project_details_graphql(
    gl_instance: gitlab.Gitlab,
    project_full_path: str,
    default_branch: Optional[str] = "main", # Fallback, ideally get from REST stub or initial GQL query part
    logger_instance: Optional[logging.Logger] = None # Accept a logger instance
) -> Optional[Dict[str, Any]]:
    """Fetches comprehensive project details using GitLab GraphQL."""

    # Dynamically insert blob queries for README and CODEOWNERS
    # Use passed-in logger or get a default one for this module
    current_logger = logger_instance if logger_instance else logger

    # And the default branch for the tree query
    # A more robust way would be to get default_branch first, then query files.
    # For simplicity here, we assume a common default_branch or it's passed.
    # If default_branch is None or incorrect, file fetching might fail or fetch from a non-default branch.
    # The REST API project object usually has `default_branch`.

    readme_blob_queries = build_gitlab_blob_queries(COMMON_README_PATHS_GITLAB, "readme")
    codeowners_blob_queries = build_gitlab_blob_queries(COMMON_CODEOWNERS_PATHS_GITLAB, "codeowners")
    
    # If default_branch is not known, we might need a preliminary query or rely on the REST stub.
    # For this example, let's assume it's passed or a common one is used.
    # The provided query template already includes `rootRef`. The processing logic will use it.
    # The `default_branch` parameter here is crucial for the `tree(ref: ...)` part.

    query_string = COMPREHENSIVE_PROJECT_QUERY_TEMPLATE.format(
        default_branch_placeholder=default_branch or "main", # Use a sensible default if not provided
        readme_blobs_placeholder=readme_blob_queries,
        codeowners_blobs_placeholder=codeowners_blob_queries
    )

    # The python-gitlab library's graphql.execute method takes the query string directly.
    params = {"fullPath": project_full_path}

    try:
        current_logger.debug(f"Executing GitLab GraphQL query for {project_full_path}")
        
        
        # Instantiate gitlab.GraphQL using details from the gl_instance
        # The gl_instance.private_token is the PAT, which is passed as 'token' to gitlab.GraphQL
        if not hasattr(gitlab, 'GraphQL'):
            current_logger.error("gitlab.GraphQL class is not available in the installed python-gitlab version. Cannot proceed.")
            return None
        
        # Use gitlab.GraphQL class and its execute method as per successful test
        gq_client = gitlab.GraphQL(url=gl_instance.url, token=gl_instance.private_token, ssl_verify=gl_instance.ssl_verify)
        result = gq_client.execute(query_string, variable_values=params)


        # Check for GraphQL errors within a successful HTTP response
        if "errors" in result and result["errors"]:
            current_logger.error(f"GitLab GraphQL query for {project_full_path} returned errors in response: {result['errors']}")
            return None
        current_logger.debug(f"Successfully fetched GitLab GraphQL data for {project_full_path}")
        return result.get("project")
    except gitlab.exceptions.GitlabHttpError as e_http:
        current_logger.error(
            f"GitLab GraphQL HTTP error for {project_full_path}: "
            f"Status Code: {e_http.response_code}, Message: {e_http.error_message}"
        )
    except gitlab.exceptions.GitlabError as e_gitlab: # Catch other general GitLab API errors
        current_logger.error(
            f"General GitLab API error during GraphQL fetch for {project_full_path}: {e_gitlab}"
        )    
    except Exception as e:
        current_logger.error(f"Unexpected error during GitLab GraphQL fetch for {project_full_path}: {e}", exc_info=True)
    return None


def fetch_commit_history_graphql(
    gl_instance: gitlab.Gitlab,
    project_full_path: str,
    default_branch_name: Optional[str] = None, # If None, GQL will use project's default
    max_commits_to_fetch_for_labor: int = 5000, # Safety limit
    logger_instance: Optional[logging.Logger] = None # Accept a logger instance
) -> List[Tuple[str, str, str]]: # (author_name, author_email, commit_date_iso_string)
    """
    Fetches commit history for a GitLab project using GraphQL, handling pagination.
    """
    all_commits_data: List[Tuple[str, str, str]] = []
    fetched_count = 0
    page_num = 1
    commits_per_page = 100 # Standard per_page limit for GitLab API

    # Use passed-in logger or get a default one for this module
    current_logger = logger_instance if logger_instance else logger

    current_logger.info(f"Fetching commit history for project (branch: {default_branch_name or 'default'}) via REST API.")

    try:
        project = gl_instance.projects.get(project_full_path, lazy=True)
        
        while fetched_count < max_commits_to_fetch_for_labor:
            # Fetch a page of commits
            commits_page = project.commits.list(
                ref_name=default_branch_name, # Uses project's default branch if None
                page=page_num,
                per_page=commits_per_page,
                all=False # Important for manual pagination control
            )

            if not commits_page: # No more commits or branch not found
                if page_num == 1: # First page was empty
                    current_logger.warning(f"No commit history found or branch '{default_branch_name or 'default'}' not found via REST API.")
                break

            for commit_obj in commits_page:
                if fetched_count >= max_commits_to_fetch_for_labor:
                    break
                
                author_name = commit_obj.author_name if commit_obj.author_name is not None else "Unknown Author"
                author_email = commit_obj.author_email if commit_obj.author_email is not None else "unknown@example.com"
                authored_date = commit_obj.authored_date # This is already an ISO 8601 string
                
                if author_name and author_email and authored_date:
                    all_commits_data.append((author_name, author_email, authored_date))
                    fetched_count += 1
            
            current_logger.debug(f"Fetched page {page_num} of {len(commits_page)} commits. Total so far: {fetched_count}.")

            if len(commits_page) < commits_per_page or fetched_count >= max_commits_to_fetch_for_labor:
                break # Last page reached or limit met
            
            page_num += 1

    except gitlab.exceptions.GitlabGetError as e_get:
        current_logger.error(f"REST API error getting project for commit history: {e_get}")
    except gitlab.exceptions.GitlabListError as e_list:
        current_logger.error(f"REST API error listing commits: {e_list}")
    except gitlab.exceptions.GitlabHttpError as e_http: # Catch other HTTP errors
        try:
            current_logger.error(
                f"REST API HTTP error for commit history: "
                f"Status Code: {e_http.response_code}, Message: {e_http.error_message}"
            )
        except gitlab.exceptions.GitlabError as e_gitlab: # Catch other general GitLab API errors
            current_logger.error(
                f"General API error fetching commit history: {e_gitlab}"
            )
        except Exception as e:
            current_logger.error(f"Unexpected error fetching commit history: {e}", exc_info=True)
            
    current_logger.info(f"Fetched a total of {len(all_commits_data)} commit data entries via REST API.")
    return all_commits_data
