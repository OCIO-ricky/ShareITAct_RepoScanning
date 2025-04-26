# github_connector.py
import os
import logging
from datetime import datetime # Ensure datetime is imported
# Make sure to import specific exceptions
from github import Github, BadCredentialsException, UnknownObjectException, GithubException
# Import requests exceptions if PyGithub uses it under the hood for network errors
from requests.exceptions import RequestException

logger = logging.getLogger(__name__)

# Placeholder check helper (optional, but cleaner)
def is_placeholder_token(token):
    """Checks if the token is missing or likely a placeholder."""
    # Add more robust checks if needed (e.g., length, specific patterns)
    # This basic check assumes 'ghp_' tokens are usually longer than 40 chars if real
    return not token or (token.startswith("ghp_") and len(token) < 40)

def fetch_repositories(token, org_name) -> list[dict]:
    """Fetches all repositories for the configured organization using PyGithub."""
    # --- Check for placeholder/missing token/org ---
    if is_placeholder_token(token):
        logger.info("GitHub token is missing or appears to be a placeholder. Skipping GitHub scan.")
        return []
    if not org_name:
        logger.warning("GitHub organization name not provided. Skipping GitHub scan.")
        return []
    # --- End Check ---

    repos_data = []
    g = None
    try:
        # --- Initialize SDK inside ---
        logger.info(f"Attempting to connect to GitHub API...")
        # Optional: Add base_url for GitHub Enterprise
        # github_api_url = os.getenv("GITHUB_API_URL")
        # g = Github(base_url=github_api_url, login_or_token=token) if github_api_url else Github(login_or_token=token)
        g = Github(login_or_token=token)
        user = g.get_user() # Verify authentication works early
        logger.info(f"GitHub SDK initialized and authenticated as user: {user.login}")
        # --- End Initialization ---

        logger.info(f"Fetching repositories for GitHub organization: {org_name} ..")
        org = g.get_organization(org_name)
        repos = org.get_repos(type='all') # PaginatedList

        count = 0
        log_interval = 50 # Log progress every 50 repositories

        # Use enumerate for easier counting if total isn't readily available
        for i, repo in enumerate(repos):
             count = i + 1
             try:

                 # Convert datetime objects to ISO strings for JSON serialization
                 created_at_iso = repo.created_at.isoformat() if repo.created_at else None
                 updated_at_iso = repo.updated_at.isoformat() if repo.updated_at else None
                 pushed_at_iso = repo.pushed_at.isoformat() if repo.pushed_at else None

                 # Create dictionary using the common keys ('repo_name', 'org_name')
                 repos_data.append({
                     'source': 'GitHub',
                     'id': repo.id,
                     'repo_name': repo.name,      # Use 'repo_name'
                     'full_name': repo.full_name,
                     'description': repo.description or '',
                     'url': repo.html_url,        # Renamed from html_url for consistency? Check generate_codejson
                     'html_url': repo.html_url,   # Keep html_url if needed elsewhere
                     'api_url': repo.url,
                     'is_private': repo.private,
                     'org_name': repo.owner.login, # Use 'org_name'
                     'created_at': created_at_iso,
                     'updated_at': updated_at_iso,
                     'pushed_at': pushed_at_iso,   # Use 'pushed_at' instead of 'last_updated'? Check generate_codejson
                     'last_updated': pushed_at_iso, # Keep last_updated if needed
                     'languages_url': repo.languages_url,
                     'tags_url': repo.tags_url,
                     'contents_url': repo.contents_url.replace('{+path}', ''),
                     'commits_url': repo.commits_url.replace('{/sha}', ''),
                     'license': {'name': repo.license.name, 'key': repo.license.key} if repo.license else None,
                     # Add fields from your common format
                     "default_branch": repo.default_branch,
                     "language": None, # Placeholder - requires separate API call (repo.get_languages())
                     "readme_url": None, # Placeholder - requires separate API call (repo.get_readme())
                     "contact_email": None, # Placeholder - logic to determine this needed
                     "exempted": False, # Placeholder
                     "exemption_reason": None # Placeholder
                 })

             except Exception as repo_err:
                 # Log error for individual repo processing but continue
                 logger.error(f"Error processing GitHub repository '{repo.name}': {repo_err}", exc_info=True)
                 # Continue to the next repository

        # Log final count after the loop
        logger.info(f"Successfully fetched details for {count} total repositories from GitHub organization '{org_name}'.")

    # --- Specific Exception Handling ---
    except BadCredentialsException:
        logger.error(f"GitHub authentication failed (401 Bad Credentials). Please check your GITHUB_TOKEN. Skipping GitHub scan.")
        return [] # Return empty list
    except UnknownObjectException:
        logger.error(f"GitHub organization '{org_name}' not found (404 Not Found). Please check your GITHUB_ORG. Skipping GitHub scan.")
        return [] # Return empty list
    except GithubException as e:
        # Catch other specific GitHub API errors
        logger.error(f"A GitHub API error occurred: {e.status} {e.data.get('message', '')}. Skipping GitHub scan.", exc_info=True)
        return [] # Return empty list
    except RequestException as e:
        # Catch network-related errors
        logger.error(f"A network error occurred connecting to GitHub: {e}. Skipping GitHub scan.", exc_info=True)
        return [] # Return empty list
    except Exception as e:
        # Catch any other unexpected errors during initialization or fetching
        logger.error(f"An unexpected error occurred during GitHub fetch for org '{org_name}': {e}. Skipping GitHub scan.", exc_info=True)
        return [] # Return empty list

    return repos_data

# --- Placeholder/Example functions for fetching additional details ---
# These would typically be called within the main loop or afterwards,
# potentially adding performance overhead due to extra API calls per repo.

# def fetch_readme_content(repo_sdk_obj):
#     """Fetches README content for a given PyGithub repository object."""
#     try:
#         content_file = repo_sdk_obj.get_readme()
#         return content_file.decoded_content.decode('utf-8')
#     except GithubException as e:
#         if e.status == 404:
#             logger.debug(f"No README found for repository: {repo_sdk_obj.full_name}")
#         else:
#             logger.error(f"API error fetching README for {repo_sdk_obj.full_name}: {e}")
#         return None
#     except Exception as e:
#          logger.error(f"Error decoding README for {repo_sdk_obj.full_name}: {e}")
#          return None

# def fetch_languages(repo_sdk_obj):
#      """Fetches language breakdown for a given PyGithub repository object."""
#      try:
#          languages = repo_sdk_obj.get_languages()
#          return languages # Returns a dict like {'Python': 12345, 'JavaScript': 6789}
#      except Exception as e:
#          logger.error(f"Error fetching languages for {repo_sdk_obj.full_name}: {e}")
#          return None

