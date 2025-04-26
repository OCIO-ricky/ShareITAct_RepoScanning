# gitlab_connector.py
import gitlab
import logging
import os
from urllib.parse import urlparse
# Import specific exceptions
from gitlab.exceptions import GitlabAuthenticationError, GitlabGetError, GitlabError
from requests.exceptions import RequestException # Assuming requests is used
from datetime import datetime # Import datetime

# Define the placeholder value  - DO NOT CHANGE THESE VALUES
PLACEHOLDER_GITLAB_TOKEN = "YOUR_GITLAB_PAT"
PLACEHOLDER_GITLAB_GROUP = "YourGitLabGroupNameOrID" # Add placeholder for group

logger = logging.getLogger(__name__)

def get_gitlab_api_url():
    """Gets the GitLab API URL from environment variables, defaulting to gitlab.com."""
    base_url = os.getenv("GITLAB_API_URL", "https://gitlab.com").rstrip('/')
    # Ensure it points to the API endpoint if just the base URL is given
    if not base_url.endswith('/api/v4'):
        # Handle common case where only domain is provided
        if urlparse(base_url).path == '':
             return f"{base_url}/api/v4"
        # Otherwise, assume the provided URL is correct (e.g., for self-hosted with specific path)
    return base_url

def fetch_repositories(token, group_name_or_id):
    """Fetches repository details from a specific GitLab group."""

    # --- Simplified Check (assuming .env is fixed) ---
    # Keep the debug logging for now to verify
    logger.debug(f"Checking GitLab token. Received token: '{token}'")
    logger.debug(f"Comparing with placeholder: '{PLACEHOLDER_GITLAB_TOKEN}'")
    if not token or token == PLACEHOLDER_GITLAB_TOKEN:
        logger.info("GitLab token is missing or is a placeholder. Skipping GitLab scan.")
        return []

    logger.debug(f"Checking GitLab group. Received group: '{group_name_or_id}'")
    if not group_name_or_id or group_name_or_id == PLACEHOLDER_GITLAB_GROUP:
        logger.info("GitLab group is missing or is a placeholder. Skipping GitLab scan.")
        return []
    # --- End Check ---

    # --- Use ORIGINAL token and group_name_or_id for API calls ---
    API_URL = get_gitlab_api_url()
    repositories = []
    gl = None

    try:
        # --- SDK Initialization ---
        logger.info(f"Attempting to connect to GitLab API at {API_URL}")
        ssl_verify_flag = os.getenv("GITLAB_SSL_VERIFY", "true").lower() != "false"
        if not ssl_verify_flag:
             logger.warning("GitLab SSL verification is DISABLED.")

        # Use the original token value for authentication
        gl = gitlab.Gitlab(API_URL, private_token=token, ssl_verify=ssl_verify_flag)
        # --- Authentication Attempt ---
        gl.auth()
        logger.info("GitLab SDK initialized and authenticated.")
        # --- End Authentication Attempt ---

        # Find the group using original group_name_or_id
        group = None
        try:
            # Try fetching by ID first if it looks like an integer
            group = gl.groups.get(int(group_name_or_id))
        except (ValueError, GitlabGetError):
            # If not an integer ID or not found by ID, search by name/path
            groups = gl.groups.list(search=group_name_or_id, top_level_only=True)
            if not groups:
                logger.error(f"GitLab group '{group_name_or_id}' not found (404 Not Found). Please check GITLAB_GROUP. Skipping GitLab scan.")
                return []
            if len(groups) > 1:
                logger.warning(f"Multiple GitLab groups found for '{group_name_or_id}'. Using the first one: {groups[0].name}")
            group = groups[0]
        except Exception as group_find_err:
             logger.error(f"Error finding GitLab group '{group_name_or_id}': {group_find_err}. Skipping GitLab scan.", exc_info=True)
             return []


        logger.info(f"Fetching projects from GitLab group: {group.name} (ID: {group.id}...)")
        projects = group.projects.list(all=True, include_subgroups=True)

        count = 0
        log_interval = 50 # Log progress every 50 repositories

        for i, project in enumerate(projects):
            count = i + 1
            try:

                # Convert datetime objects to ISO strings
                last_activity_at_iso = project.last_activity_at.isoformat() if project.last_activity_at else None

                # Basic repo info - Adapt to your common format
                repo_info = {
                    "source": "GitLab",
                    "org_name": group.full_path, # Use group path as org identifier
                    "repo_name": project.path_with_namespace,
                    "description": project.description,
                    "html_url": project.web_url,
                    "api_url": project.links['self'], # API URL for the project
                    "is_private": project.visibility == 'private',
                    "is_fork": project.forked_from_project is not None,
                    "last_updated": last_activity_at_iso,
                    "default_branch": project.default_branch,
                    "language": None, # GitLab API v4 project details don't directly list primary language easily
                    "license_name": project.license['name'] if project.license else None,
                    "readme_url": project.readme_url,
                    "contact_email": None, # Placeholder, logic to determine this needed
                    "exempted": False, # Placeholder
                    "exemption_reason": None # Placeholder
                }
                repositories.append(repo_info)
            except Exception as proj_err:
                 logger.error(f"Error processing GitLab project '{project.path_with_namespace}': {proj_err}", exc_info=True)
                 # Continue to the next project


        logger.info(f"Successfully fetched details for {len(repositories)} total repositories from GitLab group '{group.name}'.")

    # --- Specific Exception Handling ---
    except GitlabAuthenticationError:
        logger.error("GitLab authentication failed (401 Unauthorized). Please check your GITLAB_TOKEN. Skipping GitLab scan.")
        return []
    except GitlabGetError as e:
        logger.error(f"GitLab API error (404 Not Found for a resource?): {e}. Skipping GitLab scan.", exc_info=True)
        return []
    except GitlabError as e:
        logger.error(f"A general GitLab API error occurred: {e}. Skipping GitLab scan.", exc_info=True)
        return []
    except RequestException as e:
        logger.error(f"A network error occurred connecting to GitLab: {e}. Skipping GitLab scan.", exc_info=True)
        return []
    except Exception as e:
        logger.error(f"An unexpected error occurred during GitLab fetch for group '{group_name_or_id}': {e}. Skipping GitLab scan.", exc_info=True)
        return []

    return repositories
