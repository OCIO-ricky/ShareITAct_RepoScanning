# clients/gitlab_connector.py
"""
GitLab Connector for Share IT Act Repository Scanning Tool.

This module is responsible for fetching repository data from a GitLab instance,
including metadata, README content, CODEOWNERS files (if found), topics (tags),
and Git tags. It interacts with the GitLab API via the python-gitlab library.
"""

import os
import logging
import base64
from typing import List, Optional, Dict, Any
from datetime import timezone, datetime 
from utils.labor_hrs_estimator import analyze_gitlab_repo_sync # Import the labor hrs estimator


import gitlab # python-gitlab library
from gitlab.exceptions import GitlabAuthenticationError, GitlabGetError, GitlabListError

# Attempt to import the exemption processor
try:
    from utils import exemption_processor
except ImportError:
    # Provide a mock if not found, so the connector can still be outlined
    logging.getLogger(__name__).error(
        "Failed to import exemption_processor from utils. "
        "Exemption processing will be skipped by the GitLab connector (using mock)."
    )
    class MockExemptionProcessor:
        def process_repository_exemptions(self, repo_data: Dict[str, Any], default_org_identifiers: Optional[List[str]] = None) -> Dict[str, Any]:
            # This mock modifies repo_data in place and returns it,
            # ensuring it has expected keys if exemption_processor would add them.
            repo_data.setdefault('_status_from_readme', None)
            repo_data.setdefault('_private_contact_emails', [])
            repo_data.setdefault('contact', {})
            repo_data.setdefault('permissions', {"usageType": "openSource"}) # Default
            # Mock processor also removes these if the real one does
            repo_data.pop('readme_content', None)
            repo_data.pop('_codeowners_content', None)
            return repo_data
    exemption_processor = MockExemptionProcessor()

# load_dotenv() # No longer loading .env directly for auth in this connector
logger = logging.getLogger(__name__)

PLACEHOLDER_GITLAB_TOKEN = "YOUR_GITLAB_PAT" # Common placeholder for GitLab PAT

def is_placeholder_token(token: Optional[str]) -> bool:
    """Checks if the GitLab token is missing or a known placeholder."""
    return not token or token == PLACEHOLDER_GITLAB_TOKEN


def _get_readme_content_gitlab(project_obj) -> tuple[Optional[str], Optional[str]]:
    """
    Fetches and decodes the README content for a given GitLab project object.
    Tries common README filenames. Returns content and URL.
    """
    common_readme_names = ["README.md", "README.txt", "README", "readme.md"]
    if not project_obj.default_branch:
        logger.warning(f"Cannot fetch README for {project_obj.path_with_namespace}: No default branch set.")
        return None, None

    for readme_name in common_readme_names:
        try:
            readme_file = project_obj.files.get(file_path=readme_name, ref=project_obj.default_branch)
            readme_content_bytes = base64.b64decode(readme_file.content)
            readme_content_str = readme_content_bytes.decode('utf-8', errors='replace')
            readme_url = f"{project_obj.web_url}/-/blob/{project_obj.default_branch}/{readme_name.lstrip('/')}"
            logger.debug(f"Successfully fetched README '{readme_name}' for {project_obj.path_with_namespace}")
            return readme_content_str, readme_url
        except GitlabGetError as e:
            if e.response_code == 404:
                logger.debug(f"README '{readme_name}' not found in {project_obj.path_with_namespace}")
                continue
            else:
                logger.error(f"GitLab API error fetching README '{readme_name}' for {project_obj.path_with_namespace}: {e}", exc_info=False)
                return None, None 
        except Exception as e:
            logger.error(f"Unexpected error decoding README '{readme_name}' for {project_obj.path_with_namespace}: {e}", exc_info=True)
            return None, None
    logger.debug(f"No common README file found for {project_obj.path_with_namespace}")
    return None, None


def _get_codeowners_content_gitlab(project_obj) -> Optional[str]:
    """Fetches CODEOWNERS content from standard locations in a GitLab project."""
    common_paths = ["CODEOWNERS", ".gitlab/CODEOWNERS", "docs/CODEOWNERS"]
    if not project_obj.default_branch:
        logger.warning(f"Cannot fetch CODEOWNERS for {project_obj.path_with_namespace}: No default branch set.")
        return None

    for path in common_paths:
        try:
            content_file = project_obj.files.get(file_path=path.lstrip('/'), ref=project_obj.default_branch)
            content_bytes = base64.b64decode(content_file.content)
            content_str = content_bytes.decode('utf-8', errors='replace')
            logger.debug(f"Successfully fetched CODEOWNERS from '{path}' for {project_obj.path_with_namespace}")
            return content_str
        except GitlabGetError as e:
            if e.response_code == 404:
                continue
            else:
                logger.error(f"GitLab API error fetching CODEOWNERS at {path} for {project_obj.path_with_namespace}: {e}", exc_info=False)
                return None
        except Exception as e:
            logger.error(f"Unexpected error fetching CODEOWNERS at {path} for {project_obj.path_with_namespace}: {e}", exc_info=True)
            return None
    logger.debug(f"No CODEOWNERS file found in standard locations for {project_obj.path_with_namespace}")
    return None


def _fetch_tags_gitlab(project_obj) -> List[str]:
    """Fetches Git tag names using the python-gitlab project object."""
    tag_names = []
    try:
        logger.debug(f"Fetching Git tags for project: {project_obj.path_with_namespace}")
        tags = project_obj.tags.list(all=True) 
        tag_names = [tag.name for tag in tags if tag.name]
        logger.debug(f"Found {len(tag_names)} Git tags for {project_obj.path_with_namespace}")
    except GitlabListError as e:
         logger.error(f"GitLab API error listing Git tags for {project_obj.path_with_namespace}: {e}", exc_info=False)
    except Exception as e:
        logger.error(f"Unexpected error fetching Git tags for {project_obj.path_with_namespace}: {e}", exc_info=True)
    return tag_names


def fetch_repositories(
    token: Optional[str], 
    group_path: str, 
    processed_counter: List[int], 
    debug_limit: int | None = None, 
    gitlab_instance_url: str | None = None,
    hours_per_commit: Optional[float] = None) -> list[dict]:
    """
    Fetches repository (project) details from a specific GitLab group.

    Args:
        token: The GitLab Personal Access Token.
        group_path: The full path of the GitLab group (e.g., 'my-org/my-subgroup').
        processed_counter: Mutable list to track processed repositories for debug limit.
        debug_limit: Optional global limit for repositories to process.
        gitlab_instance_url: The base URL of the GitLab instance. Defaults to https://gitlab.com if None.


    Returns:
        A list of dictionaries, each containing processed metadata for a repository.
    """
    # Use a default GitLab URL if none is provided or if it's an empty string
    effective_gitlab_url = gitlab_instance_url
    if not effective_gitlab_url: # Handles both None and empty string
        effective_gitlab_url = "https://gitlab.com"
        logger.warning(f"No GitLab instance URL provided or it was empty. Using default: {effective_gitlab_url}")
    
    logger.info(f"Attempting to fetch repositories for GitLab group: {group_path} on {effective_gitlab_url}")

    if is_placeholder_token(token): # is_placeholder_token now takes token as arg
        logger.error("GitLab token is a placeholder or missing. Cannot fetch repositories.")
        return []
    if not group_path:
        logger.warning("GitLab group path not provided. Skipping GitLab scan.")
        return []

    processed_repo_list: List[Dict[str, Any]] = []

    try:
        gl = gitlab.Gitlab(effective_gitlab_url.strip('/'), private_token=token, timeout=30)
        gl.auth() 
        logger.info(f"Successfully connected and authenticated to GitLab instance: {effective_gitlab_url}")

        group = gl.groups.get(group_path, lazy=False)
        logger.info(f"Successfully found GitLab group: {group.full_path} (ID: {group.id})")

        projects_iterator = group.projects.list(all=True, include_subgroups=True, statistics=True, lazy=True)
        project_count_for_group = 0

        for proj_stub in projects_iterator:
            if debug_limit is not None and processed_counter[0] >= debug_limit:
                logger.info(f"Global debug limit ({debug_limit}) reached. Stopping repository fetching for {group_path}.")
                break
            
            repo_data: Dict[str, Any] = {} 
            try:
                # Get full project object
                project = gl.projects.get(proj_stub.id, lazy=False, statistics=True)
                repo_full_name = project.path_with_namespace
                logger.info(f"Processing repository: {repo_full_name}")
                project_count_for_group += 1
                
                if hasattr(project, 'forked_from_project') and project.forked_from_project:
                    logger.info(f"Skipping forked repository: {repo_full_name}")
                    continue
                
                repo_description = project.description if project.description else ""
                
                visibility_status = project.visibility
                if visibility_status not in ["public", "private", "internal"]:
                    logger.warning(f"Unknown visibility '{visibility_status}' for {repo_full_name}. Defaulting to 'private'.")
                    visibility_status = "private"

                created_at_dt: Optional[datetime] = None
                if project.created_at:
                    try:
                        created_at_dt = datetime.fromisoformat(project.created_at.replace('Z', '+00:00')).replace(tzinfo=timezone.utc)
                    except ValueError:
                        logger.warning(f"Could not parse created_at date string '{project.created_at}' for {repo_full_name}")

                last_activity_at_dt: Optional[datetime] = None
                if project.last_activity_at:
                    try:
                        last_activity_at_dt = datetime.fromisoformat(project.last_activity_at.replace('Z', '+00:00')).replace(tzinfo=timezone.utc)
                    except ValueError:
                        logger.warning(f"Could not parse last_activity_at date string '{project.last_activity_at}' for {repo_full_name}")
                
                all_languages_list = []
                try:
                    languages_dict = project.languages() # This is a method call
                    if languages_dict:
                        all_languages_list = list(languages_dict.keys())
                except Exception as lang_err:
                    logger.warning(f"Could not fetch languages for {repo_full_name}: {lang_err}", exc_info=False)

                readme_content, readme_html_url = _get_readme_content_gitlab(project)
                codeowners_content = _get_codeowners_content_gitlab(project)
                repo_topics = project.tag_list if hasattr(project, 'tag_list') else [] # GitLab calls them 'tag_list' for topics
                repo_git_tags = _fetch_tags_gitlab(project) # Actual Git tags

                licenses_list = []
                if hasattr(project, 'license') and project.license and isinstance(project.license, dict):
                    license_name = project.license.get('name')
                    spdx_id = project.license.get('key') 
                    license_url = project.license.get('html_url') 

                    license_entry = {}
                    if spdx_id:
                        license_entry["spdxID"] = spdx_id
                    if license_name: 
                        license_entry["name"] = license_name
                    if license_url: # This is often the URL to the license file in the repo on GitLab
                        license_entry["URL"] = license_url
                    
                    if license_entry: 
                        licenses_list.append(license_entry)

                repo_data = {
                    "name": project.path, # Name of the project within its namespace
                    "organization": group.full_path, # Full path of the parent group
                    "description": repo_description,
                    "repositoryURL": project.web_url, 
                    "homepageURL": project.web_url, 
                    "downloadURL": None,
                    "vcs": "git",
                    "repositoryVisibility": visibility_status,
                    "status": "development", 
                    "version": "N/A",      
                    "laborHours": 0,       
                    "languages": all_languages_list,
                    "tags": repo_topics,
                    "date": {
                        "created": created_at_dt.isoformat() if created_at_dt else None,
                        "lastModified": last_activity_at_dt.isoformat() if last_activity_at_dt else None,
                    },
                    "permissions": {
                        "usageType": "openSource", 
                        "exemptionText": None,
                        "licenses": licenses_list
                    },
                    "contact": {}, 
                    "contractNumber": None, 
                    "readme_content": readme_content,
                    "_codeowners_content": codeowners_content,
                    "repo_id": project.id, 
                    "readme_url": readme_html_url, 
                    "_api_tags": repo_git_tags, 
                    "archived": project.archived,  
                }
                
                if hours_per_commit is not None:
                    logger.debug(f"Estimating labor hours for GitLab repo: {project.path_with_namespace}")
                    try:
 
                        labor_df = analyze_gitlab_repo_sync(
                            project_id=str(project.id), # Ensure project_id is a string
                            token=token, # Still pass token for estimator's internal fallback
                            hours_per_commit=hours_per_commit,
                            gitlab_api_url=effective_gitlab_url,
                            session=None # Let the estimator manage its own session
                        )
                        if not labor_df.empty:
                            repo_data["laborHours"] = round(float(labor_df["EstimatedHours"].sum()), 2)
                            logger.info(f"Estimated labor hours for {project.path_with_namespace}: {repo_data['laborHours']}")
                        else:
                            repo_data["laborHours"] = 0.0
                    except Exception as e_lh:
                        logger.warning(f"Could not estimate labor hours for {project.path_with_namespace}: {e_lh}", exc_info=True)
                        repo_data["laborHours"] = 0.0 # Default or None if preferred

                # Pass default identifiers for organization context to exemption_processor
                repo_data = exemption_processor.process_repository_exemptions(repo_data, default_org_identifiers=[group.full_path])
                
                processed_repo_list.append(repo_data)
                processed_counter[0] += 1

            except GitlabGetError as p_get_err: # Error fetching full project details
                logger.error(f"GitLab API error getting full details for project stub {proj_stub.id} ({proj_stub.path_with_namespace}): {p_get_err}. Skipping.", exc_info=False)
                processed_repo_list.append({"name": proj_stub.path, "organization": group.full_path, "processing_error": f"GitLab API Error getting details: {p_get_err.error_message}"})
            except Exception as e_proj:
                logger.error(f"Unexpected error processing project stub {proj_stub.id} ({proj_stub.path_with_namespace}): {e_proj}. Skipping.", exc_info=True)
                processed_repo_list.append({"name": proj_stub.path, "organization": group.full_path, "processing_error": f"Unexpected Error: {e_proj}"})
        
        logger.info(f"Fetched and initiated processing for {project_count_for_group} projects from GitLab group: {group_path}")

    except GitlabAuthenticationError:
        logger.critical(f"GitLab authentication failed for URL {effective_gitlab_url}. Check token. Skipping GitLab scan for {group_path}.")
        # No need to append to processed_repo_list here, just return what we have or empty
    except GitlabGetError as e: # Error getting the initial group
        logger.critical(f"GitLab API error: Could not find group '{group_path}' on {effective_gitlab_url} or other API issue: {e.error_message} (Status: {e.response_code}). Skipping GitLab scan.", exc_info=False)
    except GitlabListError as e: # Error listing projects in the group
        logger.critical(f"GitLab API error listing projects for group '{group_path}' on {effective_gitlab_url}: {e.error_message}. Skipping GitLab scan.", exc_info=False)
    except Exception as e: # Catch-all for other unexpected errors during setup or group iteration
        logger.critical(f"An unexpected error occurred during GitLab connection or group processing for '{group_path}' on {effective_gitlab_url}: {e}", exc_info=True)

    return processed_repo_list


if __name__ == '__main__':
    # This basic test block will use environment variables for token, URL, and group
    # This is for direct testing of the connector, not via generate_codejson.py
    from dotenv import load_dotenv as load_dotenv_for_test # Alias to avoid conflict
    load_dotenv_for_test() # Load .env for test execution

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # For testing, get these from .env
    test_gl_token = os.getenv("GITLAB_TOKEN") 
    test_gl_url_env = os.getenv("GITLAB_URL", "https://gitlab.com")
    test_group_paths_str = os.getenv("GITLAB_GROUPS", "")
    test_group_path = test_group_paths_str.split(',')[0].strip() if test_group_paths_str else None

    if not test_gl_token or is_placeholder_token(test_gl_token): # Use the function with the token
        logger.error("Test GitLab token (GITLAB_TOKEN) not found or is a placeholder in .env.")
    elif not test_group_path:
        logger.error("No GitLab group found in GITLAB_GROUPS in .env for testing.")
    else:
        logger.info(f"--- Testing GitLab Connector for group: {test_group_path} on {test_gl_url_env} ---")
        counter = [0]
        repositories = fetch_repositories(
            token=test_gl_token, 
            group_path=test_group_path, 
            processed_counter=counter, 
            debug_limit=None, 
            gitlab_instance_url=test_gl_url_env
        )

        if repositories:
            logger.info(f"Successfully fetched {len(repositories)} repositories.")
            for i, repo_info in enumerate(repositories[:3]):
                logger.info(f"--- Repository {i+1} ({repo_info.get('name')}) ---")
                logger.info(f"  Repo ID: {repo_info.get('repo_id')}")
                logger.info(f"  Name: {repo_info.get('name')}")
                logger.info(f"  Org: {repo_info.get('organization')}")
                logger.info(f"  Description: {repo_info.get('description')}")
                logger.info(f"  Visibility: {repo_info.get('repositoryVisibility')}")
                logger.info(f"  Archived (temp): {repo_info.get('archived')}")
                logger.info(f"  API Tags (temp): {repo_info.get('_api_tags')}")
                logger.info(f"  Permissions: {repo_info.get('permissions')}")
                logger.info(f"  Contact: {repo_info.get('contact')}")
                if "processing_error" in repo_info:
                    logger.error(f"  Processing Error: {repo_info['processing_error']}")
            if len(repositories) > 3:
                logger.info(f"... and {len(repositories)-3} more repositories.")
        else:
            logger.warning("No repositories fetched or an error occurred.")
        logger.info(f"Total repositories processed according to counter: {counter[0]}")
