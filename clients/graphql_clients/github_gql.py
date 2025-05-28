# clients/graphql_clients/github_gql.py
"""
GraphQL client for fetching repository data from GitHub.
"""
import logging
from typing import Optional, Dict, Any, List, Tuple

from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
from gql.transport.exceptions import TransportQueryError

logger = logging.getLogger(__name__)

GITHUB_GRAPHQL_ENDPOINT = "https://api.github.com/graphql" # Default, can be overridden for GHES

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
    # Add more fields if needed for labor hours estimation, e.g., commit history
    # defaultBranchRef {{
    #   name # Default branch name, useful for commit history query
    #   target {{
    #     ... on Commit {{
    #       history(first: 1) {{ # Just to get the branch name if not already known
    #         nodes {{
    #           oid # Latest commit SHA on default branch
    #         }}
    #       }}
    #     }}
    #   }}
    # }}
  }}
}}
""")

def fetch_repository_details_graphql(
    client: Client,
    owner: str,
    repo_name: str
) -> Optional[Dict[str, Any]]:
    """Fetches comprehensive repository details using GraphQL."""
    params = {
        "owner": owner,
        "name": repo_name,
    }
    try:
        logger.debug(f"Executing GraphQL query for {owner}/{repo_name}")
        result = client.execute(COMPREHENSIVE_REPO_QUERY, variable_values=params)
        logger.debug(f"Successfully fetched GraphQL data for {owner}/{repo_name}")
        return result.get("repository")
    except TransportQueryError as e:
        logger.error(f"GraphQL query failed for {owner}/{repo_name}: {e.errors}")
        # Check for specific rate limit errors if possible from e.errors
        # e.g., if any error['type'] == 'RATE_LIMITED'
    except Exception as e:
        logger.error(f"Unexpected error during GraphQL fetch for {owner}/{repo_name}: {e}", exc_info=True)
    return None


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
}
""")

def fetch_commit_history_graphql(
    client: Client,
    owner: str,
    repo_name: str,
    default_branch_name: Optional[str] = None,
    max_commits_to_fetch_for_labor: int = 5000, # Safety limit
    logger_instance: Optional[logging.Logger] = None # Accept a logger instance
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

    while has_next_page and fetched_count < max_commits_to_fetch_for_labor:
        params = {
            "owner": owner,
            "name": repo_name,
            "branch": branch_to_query,
            "afterCursor": current_cursor
        }
        try:
            result = client.execute(COMMIT_HISTORY_QUERY, variable_values=params)
            
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

        except TransportQueryError as e:
            current_logger.error(f"GraphQL query failed for commit history of {owner}/{repo_name}: {e.errors}")
            # Check for specific rate limit errors if possible from e.errors
            if e.errors and isinstance(e.errors, list):
                for error_detail in e.errors:
                    if isinstance(error_detail, dict) and error_detail.get('type') == 'RATE_LIMITED':
                        current_logger.warning(f"GraphQL RATE LIMIT detected during commit history fetch for {owner}/{repo_name}.")
                        # Consider adding a delay or specific retry logic here if the client's default isn't enough
                        break 
            break # Stop fetching on error
        except Exception as e:
            current_logger.error(f"Unexpected error fetching commit history for {owner}/{repo_name}: {e}", exc_info=True)
            break
    
    current_logger.info(f"Fetched a total of {len(all_commits_data)} commit data entries for {owner}/{repo_name} via GraphQL.")
    return all_commits_data
