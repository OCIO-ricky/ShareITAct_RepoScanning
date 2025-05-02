# gitlab_connector.py
import os
import logging
import gitlab
from gitlab.exceptions import GitlabAuthenticationError, GitlabGetError, GitlabListError
from datetime import datetime, timezone # Added timezone
import base64
from typing import List, Optional, Dict, Any # Added typing
# --- Import the processor ---
import utils.exemption_processor

logger = logging.getLogger(__name__)

# Placeholder check helper
def is_placeholder_token(token):
    """Checks if the token is missing or likely a placeholder."""
    return not token or token == "YOUR_GITLAB_PAT"

# --- Helper to fetch CODEOWNERS for GitLab ---
def _get_codeowners_content_gitlab(project) -> Optional[str]:
    """Fetches CODEOWNERS content from standard locations in a GitLab project."""
    # GitLab typically looks in root, .gitlab/, or docs/
    common_paths = ["CODEOWNERS", ".gitlab/CODEOWNERS", "docs/CODEOWNERS"]
    if not project.default_branch:
        logger.warning(f"Cannot fetch CODEOWNERS for {project.path_with_namespace}: No default branch set.")
        return None

    for path in common_paths:
        try:
            # Use the python-gitlab object's files.get method
            content_file = project.files.get(file_path=path, ref=project.default_branch)
            content_bytes = base64.b64decode(content_file.content)
            try:
                return content_bytes.decode('utf-8')
            except UnicodeDecodeError:
                logger.warning(f"Could not decode CODEOWNERS at {path} as UTF-8 for {project.path_with_namespace}. Trying latin-1.")
                try:
                    return content_bytes.decode('latin-1')
                except Exception:
                     logger.error(f"Failed to decode CODEOWNERS at {path} for {project.path_with_namespace} even with latin-1.")
                     return None
        except GitlabGetError as e:
            if e.response_code == 404:
                continue # File not found, try next path
            else:
                # Log other API errors
                logger.error(f"GitLab API error fetching CODEOWNERS at {path} for {project.path_with_namespace}: {e}", exc_info=True)
                return None # Stop trying on non-404 errors
        except Exception as e:
            logger.error(f"Unexpected error fetching CODEOWNERS at {path} for {project.path_with_namespace}: {e}", exc_info=True)
            return None # Stop trying on unexpected errors
    logger.debug(f"No CODEOWNERS file found in standard locations for {project.path_with_namespace}")
    return None
# --- END Helper ---

# --- Helper to fetch Tags using python-gitlab ---
def _fetch_tags_gitlab(project) -> List[str]:
    """Fetches tag names using the python-gitlab project object."""
    tag_names = []
    try:
        logger.debug(f"Fetching tags for project: {project.path_with_namespace}")
        # project.tags.list() returns a list (handles pagination via all=True)
        tags = project.tags.list(all=True)
        tag_names = [tag.name for tag in tags if tag.name]
        logger.debug(f"Found {len(tag_names)} tags for {project.path_with_namespace}")
    except GitlabListError as e:
         logger.error(f"GitLab API error listing tags for {project.path_with_namespace}: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Unexpected error fetching tags for {project.path_with_namespace}: {e}", exc_info=True)
    return tag_names
# --- END Helper ---


def fetch_repositories(token, group_name, processed_counter: list[int], debug_limit: int | None) -> list[dict]:
    """
    Fetches repository details from GitLab, processes exemptions,
    respecting a global limit, and returns a list of processed repository data dictionaries.
    """
    if is_placeholder_token(token):
        logger.info("GitLab token is missing or appears to be a placeholder. Skipping GitLab scan.")
        return []
    if not group_name:
        logger.warning("GitLab group name not provided. Skipping GitLab scan.")
        return []

    processed_repo_list = []
    gitlab_url = os.getenv("GITLAB_URL", "https://gitlab.com")
    gl = None

    try:
        logger.info(f"Attempting to connect to GitLab instance at {gitlab_url}...")
        gl = gitlab.Gitlab(gitlab_url, private_token=token, timeout=30)
        # --- Authentication Check ---
        try:
            gl.auth() # Check if authentication is successful
            logger.info("GitLab SDK initialized and authenticated.")
        except GitlabAuthenticationError as e:
            raise # Re-raise the exception to stop execution
        except Exception as e:
            # Log any other unexpected error during auth and raise it
            logger.error(f"Unexpected error during GitLab authentication: {e}. Aborting GitLab scan.")
            raise # Re-raise the exception to stop execution
        
        logger.info("GitLab SDK initialized and authenticated.")

        logger.info(f"Fetching group: {group_name}")
        groups = gl.groups.list(search=group_name, all=True)
        group = next((g for g in groups if g.full_path.lower() == group_name.lower()), None)
        if not group:
             raise GitlabGetError(error_message=f"Group '{group_name}' not found by full path.", response_code=404)

        logger.info(f"Fetching projects for GitLab group: {group.full_path} (ID: {group.id})...")
        projects = group.projects.list(all=True, include_subgroups=True, statistics=True, lazy=True)

        for pr in projects:
            if debug_limit is not None and processed_counter[0] >= debug_limit:
                logger.warning(f"--- DEBUG MODE: Global limit ({debug_limit}) reached during GitLab scan. Stopping GitLab fetch. ---")
                break
            project = gl.projects.get(pr.id) 
            repo_data = {}
            try:
                if hasattr(project, 'forked_from_project') and project.forked_from_project:
                    logger.info(f"Skipping forked repository: {project.path_with_namespace}")
                    continue
                                  
                logger.debug(f"Fetching data for GitLab project: {project.path_with_namespace}")

                created_at_iso = str(project.created_at)
                last_activity_at_iso = str(project.last_activity_at)
                repo_visibility = "private" if project.visibility == 'private' else "public"
              #  repo_language = None
                # --- Fetch ALL Languages ---
                all_languages_list = []
                try:
                    languages_dict = project.languages() # Returns dict like {'Python': 60.8, 'HTML': 39.2}
                    if languages_dict:
                        all_languages_list = list(languages_dict.keys())
                        logger.debug(f"Fetched languages for {project.path_with_namespace}: {all_languages_list}")
                    else:
                        logger.debug(f"No languages detected by API for {project.path_with_namespace}")
                except Exception as lang_err:
                    logger.error(f"Error fetching languages for {project.path_with_namespace}: {lang_err}", exc_info=True)

                licenses_list = []
                # GitLab's project object has a 'license' attribute if detected
                if hasattr(project, 'license') and project.license:
                     licenses_list.append({
                         "name": project.license.get('name', project.license.get('key')), # Prefer name, fallback to key
                         # "URL": project.license.get('url') # GitLab API might provide this
                     })
                if not licenses_list:
                    logger.debug(f"No license found via API for {project.path_with_namespace}. Applying default: Apache License 2.0")
                    licenses_list.append({"name": "Apache License 2.0", "URL": "https://www.apache.org/licenses/LICENSE-2.0"})

                # --- Fetch README Content ---
                readme_content_str: Optional[str] = None
                readme_url: Optional[str] = None
                if project.default_branch:
                    try:
                        readme_found = False
                        for readme_name in ["README.md", "README.txt", "README"]:
                            try:
                                readme_file = project.files.get(file_path=readme_name, ref=project.default_branch)
                                readme_content_bytes = base64.b64decode(readme_file.content)
                                try: readme_content_str = readme_content_bytes.decode('utf-8')
                                except UnicodeDecodeError:
                                    try: readme_content_str = readme_content_bytes.decode('latin-1')
                                    except Exception: readme_content_str = readme_content_bytes.decode('utf-8', errors='ignore')
                                readme_url = f"{project.web_url}/-/blob/{project.default_branch}/{readme_name}"
                                logger.debug(f"Fetched README '{readme_name}' for {project.path_with_namespace}")
                                readme_found = True
                                break
                            except GitlabGetError as e:
                                if e.response_code == 404: continue
                                else: raise
                        if not readme_found: logger.debug(f"No common README file found for project: {project.path_with_namespace}")
                    except Exception as readme_err: logger.error(f"Error fetching/decoding README for {project.path_with_namespace}: {readme_err}", exc_info=True)
                else: logger.debug(f"Skipping README fetch for {project.path_with_namespace} - no default branch.")

                # --- Fetch CODEOWNERS Content ---
                codeowners_content_str = _get_codeowners_content_gitlab(project)

                # --- Fetch Tags (Topics) ---
                # Use project.tag_list for GitLab's equivalent of topics
                repo_topics = project.tag_list

                # --- Fetch Git Tags ---
                repo_tags = _fetch_tags_gitlab(project)

                # --- Get Archived Status ---
                # GitLab uses 'archived' boolean attribute on the project object
                repo_archived = project.archived

                repo_data = {
                    # === Core Schema Fields ===
                    "name": project.path,
                    "description": project.description or '',
                    "organization": group.full_path,
                    "repositoryURL": project.web_url,
                    "homepageURL": project.web_url, # GitLab doesn't have a distinct homepage field easily
                    "downloadURL": None,
                    "vcs": "gitlab",
                    "repositoryVisibility": repo_visibility,
                    "status": "development", # Placeholder
                    "version": "N/A", # Placeholder
                    "laborHours": 0,
                    "languages": all_languages_list, # Populate with the full list
                    "tags": repo_topics, # Use GitLab's tag_list as topics

                    # === Nested Schema Fields ===
                    "date": {"created": created_at_iso, "lastModified": last_activity_at_iso},
                    "permissions": {"usageType": None, "exemptionText": None, "licenses": licenses_list},
                    "contact": {"name": "Centers for Disease Control and Prevention", "email": None},
                    "contractNumber": None,

                    # === Fields needed for processing ===
                    "readme_content": readme_content_str,
                    "_codeowners_content": codeowners_content_str,
                    "_is_private_flag": repo_visibility == 'private',
                    "_all_languages": all_languages_list, 

                    # === Additional Fields ===
                    "repo_id": project.id,
                    "readme_url": readme_url,
                    "_api_tags": repo_tags, # Store actual Git tags for version inference
                    "archived": repo_archived, # Store archived status
                }

                processed_data = utils.exemption_processor.process_repository_exemptions(repo_data)

                # --- Clean up temporary fields ---
                # Processor removes readme_content, _codeowners_content
                processed_data.pop('_is_private_flag', None)
                processed_data.pop('_all_languages', None) 
                # Remove fields only needed for inference later
                processed_data.pop('_api_tags', None)
                processed_data.pop('archived', None) # Remove unless needed downstream

                processed_repo_list.append(processed_data)
                processed_counter[0] += 1

            except Exception as proj_err:
                logger.error(f"Error processing GitLab project '{project.path_with_namespace}': {proj_err}", exc_info=True)
                processed_repo_list.append({
                    'name': project.path,
                    'organization': group.full_path,
                    'processing_error': f"Connector stage: {proj_err}"
                 })
                processed_counter[0] += 1

        logger.info(f"Finished GitLab scan. Processed {len(processed_repo_list)} projects. Global count: {processed_counter[0]}")

    # --- Exception Handling ---
    except GitlabAuthenticationError: logger.error(f"GitLab authentication failed. Check GITLAB_TOKEN/URL ({gitlab_url}). Skipping."); return []
    except GitlabGetError as e:
         if e.response_code == 404: logger.error(f"GitLab group '{group_name}' not found (404). Check GITLAB_GROUP. Skipping.")
         else: logger.error(f"GitLab API error fetching group '{group_name}': {e}. Skipping.", exc_info=True)
         return []
    except GitlabListError as e: logger.error(f"GitLab API error listing projects for group '{group_name}': {e}. Skipping.", exc_info=True); return []
    except Exception as e: logger.error(f"Unexpected error during GitLab fetch for group '{group_name}': {e}. Skipping.", exc_info=True); return []

    return processed_repo_list
