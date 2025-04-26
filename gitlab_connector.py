# gitlab_connector.py
import os
import logging
import gitlab
from gitlab.exceptions import GitlabAuthenticationError, GitlabGetError, GitlabListError
from datetime import datetime
import base64
# --- Import the new processor ---
import exemption_processor

logger = logging.getLogger(__name__)

# Placeholder check helper
def is_placeholder_token(token):
    """Checks if the token is missing or likely a placeholder."""
    return not token or token == "YOUR_GITLAB_PAT"

def fetch_repositories(token, group_name) -> list[dict]:
    """
    Fetches repository details from GitLab, processes exemptions,
    and returns a list of processed repository data dictionaries.
    """
    if is_placeholder_token(token):
        logger.info("GitLab token is missing or appears to be a placeholder. Skipping GitLab scan.")
        return []
    if not group_name:
        logger.warning("GitLab group name not provided. Skipping GitLab scan.")
        return []

    processed_repo_list = [] # Store final processed data
    gitlab_url = os.getenv("GITLAB_URL", "https://gitlab.com") # Default to gitlab.com
    gl = None

    try:
        logger.info(f"Attempting to connect to GitLab instance at {gitlab_url}...")
        gl = gitlab.Gitlab(gitlab_url, private_token=token, timeout=30) # Added timeout
        gl.auth() # Verify authentication
        logger.info("GitLab SDK initialized and authenticated.")

        logger.info(f"Fetching group: {group_name}")
        # Use search to potentially find group by path more reliably if ID isn't known
        groups = gl.groups.list(search=group_name, all=True)
        group = None
        for g in groups:
            # Match full path to avoid ambiguity with subgroups of same name
            if g.full_path.lower() == group_name.lower():
                group = g
                break
        if not group:
             raise GitlabGetError(error_message=f"Group '{group_name}' not found by full path.", response_code=404)

        logger.info(f"Fetching projects (repositories) for GitLab group: {group.full_path} (ID: {group.id})...")
        # include_subgroups=True might be useful depending on requirements
        projects = group.projects.list(all=True, include_subgroups=True, statistics=True, lazy=True) # Use lazy=True for generator

        count = 0
        for project in projects: # Iterate through the generator
            count += 1
            repo_data = {} # Start fresh for each project
            try:
                logger.debug(f"Fetching data for GitLab project: {project.path_with_namespace}")

                # --- Fetch Base Data ---
                created_at_iso = project.created_at.isoformat() if project.created_at else None
                last_activity_at_iso = project.last_activity_at.isoformat() if project.last_activity_at else None

                repo_data = {
                    'source': 'GitLab',
                    'id': project.id,
                    'repo_name': project.path,
                    'full_name': project.path_with_namespace,
                    'description': project.description or '',
                    'url': project.web_url,
                    'html_url': project.web_url,
                    'api_url': f"{gitlab_url}/api/v4/projects/{project.id}",
                    'is_private': project.visibility == 'private',
                    'org_name': group.full_path, # Use group full path
                    'created_at': created_at_iso,
                    'updated_at': last_activity_at_iso,
                    'pushed_at': last_activity_at_iso, # Use last_activity_at as best guess
                    'last_updated': last_activity_at_iso,
                    "default_branch": project.default_branch,
                    "language": None, # Will be fetched next
                    "readme_url": None,
                    "readme_content": None, # Will be fetched next
                    "contact_email": None,
                    # Exemption fields added by processor
                }

                # --- Fetch Language (Primary) ---
                try:
                    # statistics=True needed in project list call
                    languages = project.languages()
                    if languages:
                        repo_data['language'] = max(languages, key=languages.get)
                except Exception as lang_err:
                     logger.error(f"Error fetching languages for {project.path_with_namespace}: {lang_err}", exc_info=True)

                # --- Fetch README Content ---
                if project.default_branch: # Only try if a default branch exists
                    try:
                        # Attempt to get README, handle potential 404
                        readme_file = project.files.get(file_path='README.md', ref=project.default_branch)
                        readme_content_bytes = base64.b64decode(readme_file.content)
                        try:
                            repo_data['readme_content'] = readme_content_bytes.decode('utf-8')
                        except UnicodeDecodeError:
                            try:
                                repo_data['readme_content'] = readme_content_bytes.decode('latin-1')
                                logger.warning(f"Decoded README for {project.path_with_namespace} using latin-1.")
                            except Exception:
                                repo_data['readme_content'] = readme_content_bytes.decode('utf-8', errors='ignore')
                                logger.warning(f"Decoded README for {project.path_with_namespace} using utf-8, ignoring errors.")
                        repo_data['readme_url'] = f"{project.web_url}/-/blob/{project.default_branch}/README.md"
                        logger.debug(f"Fetched README for {project.path_with_namespace}")
                    except GitlabGetError as e:
                        if e.response_code == 404:
                            logger.debug(f"No README.md found for project: {project.path_with_namespace}")
                        else:
                            logger.error(f"GitLab API error fetching README for {project.path_with_namespace}: {e}")
                    except Exception as readme_err:
                        logger.error(f"Error fetching/decoding README for {project.path_with_namespace}: {readme_err}", exc_info=True)
                else:
                    logger.debug(f"Skipping README fetch for {project.path_with_namespace} - no default branch.")


                # --- Call Exemption Processor ---
                processed_data = exemption_processor.process_repository_exemptions(repo_data)

                # --- Clean up ---
                processed_data.pop('readme_content', None)

                # Add the fully processed data
                processed_repo_list.append(processed_data)

            except Exception as proj_err:
                logger.error(f"Error processing GitLab project '{project.path_with_namespace}': {proj_err}", exc_info=True)
                # Optionally append minimal error info
                # processed_repo_list.append({'repo_name': project.path_with_namespace, 'error': str(proj_err)})

        logger.info(f"Successfully fetched and processed {len(processed_repo_list)} total projects from GitLab group '{group_name}'.")

    except GitlabAuthenticationError:
        logger.error(f"GitLab authentication failed. Check GITLAB_TOKEN and GITLAB_URL ({gitlab_url}). Skipping.")
        return []
    except GitlabGetError as e:
         if e.response_code == 404:
              logger.error(f"GitLab group '{group_name}' not found (404). Check GITLAB_GROUP. Skipping.")
         else:
              logger.error(f"GitLab API error fetching group '{group_name}': {e}. Skipping.", exc_info=True)
         return []
    except GitlabListError as e:
         logger.error(f"GitLab API error listing projects for group '{group_name}': {e}. Skipping.", exc_info=True)
         return []
    except Exception as e:
        logger.error(f"An unexpected error occurred during GitLab fetch for group '{group_name}': {e}. Skipping.", exc_info=True)
        return []

    return processed_repo_list
