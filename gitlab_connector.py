# gitlab_connector.py
import os
import logging
import gitlab
from gitlab.exceptions import GitlabAuthenticationError, GitlabGetError, GitlabListError
from datetime import datetime
import base64
# --- Import the new processor ---
import exemption_processor # Ensure this import is present

logger = logging.getLogger(__name__)

# Placeholder check helper
def is_placeholder_token(token):
    """Checks if the token is missing or likely a placeholder."""
    return not token or token == "YOUR_GITLAB_PAT"

# --- UPDATE function signature ---
def fetch_repositories(token, group_name, processed_counter: list[int], debug_limit: int | None) -> list[dict]:
    """
    Fetches repository details from GitLab, processes exemptions,
    respecting a global limit, and returns a list of processed repository data dictionaries.

    Args:
        token: GitLab PAT.
        group_name: GitLab group name/path.
        processed_counter: A mutable list containing the current global count of processed repos.
        debug_limit: The maximum number of repos to process globally (or None).
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

        # --- Loop through projects, respecting the limit ---
        for project in projects: # Iterate through the generator
            # --- ADD DEBUG CHECK ---
            if debug_limit is not None and processed_counter[0] >= debug_limit:
                logger.warning(f"--- DEBUG MODE: Global limit ({debug_limit}) reached during GitLab scan. Stopping GitLab fetch. ---")
                break # Exit the loop over GitLab projects
            # --- END DEBUG CHECK ---

            repo_data = {} # Start fresh for each project
            try:
               # --- Add fork check early ---
                if project.forked_from_project:
                    # Log at INFO level
                    logger.info(f"Skipping forked repository: {project.path_with_namespace}")
                    continue # Move to the next repository in the loop
                # --- End fork check ---

                logger.debug(f"Fetching data for GitLab project: {project.path_with_namespace}")

                # --- Fetch Base Data ---
                created_at_iso = project.created_at.isoformat() if project.created_at else None
                last_activity_at_iso = project.last_activity_at.isoformat() if project.last_activity_at else None
                repo_visibility = "private" if project.visibility == 'private' else "public"
                repo_language = None # Will fetch later

                # --- Fetch Language (Primary) ---
                try:
                    # statistics=True needed in project list call
                    languages_dict = project.languages()
                    if languages_dict:
                        repo_language = max(languages_dict, key=languages_dict.get)
                except Exception as lang_err:
                     logger.error(f"Error fetching languages for {project.path_with_namespace}: {lang_err}", exc_info=True)

                # Prepare license structure (GitLab doesn't provide this as easily as GitHub)
                licenses_list = []
                # You might add logic here later to check for a LICENSE file if needed

                # --- ADD DEFAULT LICENSE ---
                if not licenses_list:
                    logger.debug(f"No license found via API for {project.path_with_namespace}. Applying default: Apache License 2.0")
                    licenses_list.append({
                        "name": "Apache License 2.0",
                        "URL": "https://www.apache.org/licenses/LICENSE-2.0"
                    })
                # --- END DEFAULT LICENSE ---

                # Build the dictionary using schema 2.0 field names where possible
                repo_data = {
                    # === Core Schema Fields ===
                    "name": project.path,
                    "description": project.description or '',
                    "organization": group.full_path, # Use group full path as organization
                    "repositoryURL": project.web_url,
                    "homepageURL": project.web_url, # Default to repo URL
                    "downloadURL": None, # Requires release info
                    "vcs": "git",
                    "repositoryVisibility": repo_visibility,
                    "status": "development", # Placeholder
                    "version": "N/A", # Placeholder
                    "laborHours": 0, # Placeholder
                    "languages": [repo_language] if repo_language else [], # Schema expects list
                    "tags": [], # Placeholder - requires fetching tags API

                    # === Nested Schema Fields ===
                    "date": {
                        "created": created_at_iso,
                        "lastModified": last_activity_at_iso, # Use last_activity_at as best indicator
                        # "metadataLastUpdated": Will be added globally later
                    },
                    "permissions": {
                        "usageType": None, # To be determined by exemption_processor
                        "exemptionText": None, # To be determined by exemption_processor
                        "licenses": licenses_list # Use the potentially updated list
                    },
                    "contact": {
                        "name": "Centers for Disease Control and Prevention", # Default contact name
                        "email": None # To be determined by exemption_processor
                    },
                    "contractNumber": None, # To be determined by exemption_processor

                    # === Fields needed for processing (will be removed later) ===
                    "readme_content": None, # Fetched next
                    "_is_private_flag": repo_visibility == 'private', # Temp flag
                    "_language_heuristic": repo_language, # Temp field

                    # === Original/Additional Fields (Kept at the end) ===
                    "repo_id": project.id,
                    "readme_url": None, # Will be updated after fetching README
                }


                # --- Fetch README Content ---
                if project.default_branch: # Only try if a default branch exists
                    try:
                        # Attempt to get README, handle potential 404
                        # Try common names
                        readme_found = False
                        for readme_name in ["README.md", "README.txt", "README"]:
                            try:
                                readme_file = project.files.get(file_path=readme_name, ref=project.default_branch)
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
                                # Construct a likely README URL
                                repo_data['readme_url'] = f"{project.web_url}/-/blob/{project.default_branch}/{readme_name}"
                                logger.debug(f"Fetched README '{readme_name}' for {project.path_with_namespace}")
                                readme_found = True
                                break # Found one, stop looking
                            except GitlabGetError as e:
                                if e.response_code == 404:
                                    continue # Try next filename
                                else:
                                    raise # Re-raise other GitLab errors
                        if not readme_found:
                             logger.debug(f"No common README file found for project: {project.path_with_namespace}")

                    except Exception as readme_err:
                        logger.error(f"Error fetching/decoding README for {project.path_with_namespace}: {readme_err}", exc_info=True)
                else:
                    logger.debug(f"Skipping README fetch for {project.path_with_namespace} - no default branch.")


                # --- Call Exemption Processor ---
                # Pass the repo_data dictionary, processor modifies it in place
                processed_data = exemption_processor.process_repository_exemptions(repo_data)

                # --- Clean up temporary/processed fields ---
                processed_data.pop('readme_content', None)
                processed_data.pop('_is_private_flag', None)
                processed_data.pop('_language_heuristic', None)

                # Add the fully processed data
                processed_repo_list.append(processed_data)

                # --- INCREMENT GLOBAL COUNTER ---
                processed_counter[0] += 1
                # --- END INCREMENT ---

            except Exception as proj_err:
                logger.error(f"Error processing GitLab project '{project.path_with_namespace}': {proj_err}", exc_info=True)
                # Do NOT increment counter if an error occurred

        # --- UPDATE Log Message ---
        logger.info(f"Finished GitLab scan. Processed {len(processed_repo_list)} projects in this connector. Global count: {processed_counter[0]}")

    # --- Exception Handling (as before) ---
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
