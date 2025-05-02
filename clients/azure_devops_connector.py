# azure_devops_connector.py
import logging
import os
from azure.devops.connection import Connection
from msrest.authentication import BasicAuthentication
from azure.devops.exceptions import AzureDevOpsClientRequestError, AzureDevOpsServiceError
from azure.devops.v7_0.core import CoreClient
from azure.devops.v7_0.core import models as core_models
from azure.devops.v7_0.git import GitClient
from azure.devops.v7_0.git import models as git_models
from requests.exceptions import RequestException
from datetime import datetime, timezone # Added timezone
import base64
from typing import List, Optional, Dict, Any # Added typing

# --- Import the processor ---
try:
    import utils.exemption_processor
except ImportError:
    logging.critical("Failed to import exemption_processor. Cannot proceed.")
    raise

# Placeholders
PLACEHOLDER_AZURE_TOKEN = "YOUR_AZURE_DEVOPS_PAT"
PLACEHOLDER_AZURE_ORG = "YourAzureDevOpsOrgName"
PLACEHOLDER_AZURE_PROJECT = "YourAzureDevOpsProjectName"

logger = logging.getLogger(__name__)

def get_azure_devops_org_url():
    """Constructs the Azure DevOps organization URL, returning None if placeholder."""
    org_name = os.getenv("AZURE_DEVOPS_ORG")
    if not org_name or org_name == PLACEHOLDER_AZURE_ORG: return None
    base_url = os.getenv("AZURE_DEVOPS_API_URL", "https://dev.azure.com")
    return f"{base_url.rstrip('/')}/{org_name}"

# --- Helper to fetch Tags using Azure DevOps SDK ---
def _fetch_tags_ado(git_client, project_id, repo_id) -> List[str]:
    """Fetches tag names using the Azure DevOps GitClient."""
    tag_names = []
    try:
        logger.debug(f"Fetching tags for repo ID: {repo_id} in project ID: {project_id}")
        # Get refs that start with 'refs/tags/'
        refs = git_client.get_refs(repository_id=repo_id, project=project_id, filter='refs/tags/')
        tag_names = [ref.name.replace('refs/tags/', '') for ref in refs if ref.name]
        logger.debug(f"Found {len(tag_names)} tags for repo ID {repo_id}")
    except AzureDevOpsClientRequestError as e:
         logger.error(f"Azure DevOps API error fetching tags for repo ID {repo_id}: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Unexpected error fetching tags for repo ID {repo_id}: {e}", exc_info=True)
    return tag_names
# --- END Helper ---

# --- Helper to fetch CODEOWNERS (Placeholder - Skipped for ADO) ---
def _get_codeowners_content_ado(git_client, project_id, repo_id, default_branch) -> Optional[str]:
    """Placeholder: Fetches CODEOWNERS content for Azure DevOps (Skipped due to complexity)."""
    # ADO uses branch policies, not a standard CODEOWNERS file.
    # Implementing this would require complex policy checks via API.
    logger.debug(f"CODEOWNERS fetching skipped for Azure DevOps repo ID {repo_id} (not standard feature).")
    return None
# --- END Helper ---


def fetch_repositories(token, org_name, project_name, processed_counter: list[int], debug_limit: int | None) -> list[dict]:
    """
    Fetches repository details from Azure DevOps, processes exemptions,
    respecting a global limit, and returns a list of processed repository data dictionaries.
    """
    if not token or token == PLACEHOLDER_AZURE_TOKEN:
        logger.info("Azure DevOps token is missing or placeholder. Skipping.")
        return []

    organization_url = get_azure_devops_org_url()
    if not organization_url:
         logger.info("Azure DevOps organization name not provided or placeholder. Skipping.")
         return []

    scan_specific_project = project_name and project_name != PLACEHOLDER_AZURE_PROJECT
    if project_name == PLACEHOLDER_AZURE_PROJECT: logger.info("ADO project name is placeholder, scanning all.")

    processed_repo_list = []
    connection = None

    try:
        logger.info(f"Connecting to Azure DevOps at {organization_url}")
        credentials = BasicAuthentication('', token)
        connection = Connection(base_url=organization_url, creds=credentials)
        core_client = connection.clients.get_core_client()
        git_client = connection.clients.get_git_client()
        logger.info("Azure DevOps SDK connection established.")

        projects_to_scan = []
        if scan_specific_project:
            logger.info(f"Fetching specific project: {project_name}")
            try: projects_to_scan = [core_client.get_project(project_name)]
            except AzureDevOpsClientRequestError as proj_404_err:
                 if getattr(proj_404_err, 'status_code', None) == 404: logger.error(f"ADO specific project '{project_name}' not found (404). Skipping.")
                 else: logger.error(f"ADO error fetching specific project '{project_name}': {proj_404_err}. Skipping.", exc_info=True)
                 return []
            except Exception as proj_err: logger.error(f"Unexpected error fetching specific ADO project '{project_name}': {proj_err}. Skipping.", exc_info=True); return []
        else:
            logger.info("Fetching all projects in the organization...")
            projects_to_scan = core_client.get_projects()

        if not projects_to_scan:
             logger.warning(f"No ADO projects found for org '{org_name}'" + (f" matching '{project_name}'" if scan_specific_project else "."))
             return []

        for project in projects_to_scan:
            if debug_limit is not None and processed_counter[0] >= debug_limit:
                logger.warning(f"--- DEBUG MODE: Global limit reached before ADO project '{project.name}'. Skipping. ---")
                continue

            logger.debug(f"Fetching repositories for project: {project.name}...")
            try:
                project_repos = git_client.get_repositories(project.id)
                for i, repo in enumerate(project_repos):
                    if debug_limit is not None and processed_counter[0] >= debug_limit:
                        logger.warning(f"--- DEBUG MODE: Global limit reached during ADO scan in project '{project.name}'. Stopping. ---")
                        break

                    repo_data = {}
                    try:
                        if repo.is_fork:
                            logger.info(f"Skipping forked repository: {project.name}/{repo.name}")
                            continue

                        logger.debug(f"Fetching data for ADO repo: {project.name}/{repo.name}")
                        all_languages_list = []
                        default_branch = repo.default_branch.replace('refs/heads/', '') if repo.default_branch else None
                        repo_visibility = "private" if project.visibility != core_models.ProjectVisibility.PUBLIC else "public"
                        created_at_iso = None # Not easily available
                        repo_language = None # Not easily available
                        licenses_list = []

                        # --- Fetch Last Commit Date ---
                        last_updated_iso = None
                        if default_branch:
                            try:
                                commits = git_client.get_commits(repo.id, project.id, search_criteria=git_models.GitQueryCommitsCriteria(top=1, item_version=git_models.GitVersionDescriptor(version=default_branch, version_type=git_models.GitVersionType.BRANCH)))
                                if commits:
                                    committer_date = commits[0].committer.date
                                    if isinstance(committer_date, datetime): last_updated_iso = committer_date.isoformat()
                                    elif isinstance(committer_date, str):
                                        try: last_updated_iso = datetime.fromisoformat(committer_date.replace('Z', '+00:00')).isoformat()
                                        except ValueError: logger.warning(f"Could not parse commit date string '{committer_date}' for {project.name}/{repo.name}")
                            except Exception as commit_err: logger.error(f"Error fetching last commit for {project.name}/{repo.name}: {commit_err}", exc_info=True)
                        else: logger.debug(f"Skipping last commit fetch for {project.name}/{repo.name} - no default branch.")

                        # --- Add Default License ---
                        if not licenses_list:
                            logger.debug(f"No license found via API for {project.name}/{repo.name}. Applying default: Apache License 2.0")
                            licenses_list.append({"name": "Apache License 2.0", "URL": "https://www.apache.org/licenses/LICENSE-2.0"})

                        # --- Fetch README Content ---
                        readme_content_str: Optional[str] = None
                        readme_url: Optional[str] = None
                        if default_branch:
                            try:
                                readme_found = False
                                for readme_path in ["README.md", "README.txt", "README"]:
                                    try:
                                        item = git_client.get_item(repo.id, project.id, path=f"/{readme_path}", version_descriptor=git_models.GitVersionDescriptor(version=default_branch, version_type=git_models.GitVersionType.BRANCH), include_content=True)
                                        if item and item.content:
                                            try: readme_content_str = item.content # Try direct first
                                            except Exception:
                                                try:
                                                    readme_bytes = base64.b64decode(item.content)
                                                    try: readme_content_str = readme_bytes.decode('utf-8')
                                                    except UnicodeDecodeError: readme_content_str = readme_bytes.decode('latin-1')
                                                except Exception as decode_err: logger.warning(f"Could not decode README for {project.name}/{repo.name}: {decode_err}"); readme_content_str = None
                                            if readme_content_str:
                                                readme_url = f"{repo.web_url}?path=/{readme_path}&version=GB{default_branch}&_a=contents"
                                                logger.debug(f"Fetched README '{readme_path}' for {project.name}/{repo.name}")
                                                readme_found = True; break
                                    except AzureDevOpsClientRequestError as item_404_err:
                                        if getattr(item_404_err, 'status_code', None) == 404: continue
                                        else: raise
                                if not readme_found: logger.debug(f"No common README file found for {project.name}/{repo.name}")
                            except AzureDevOpsClientRequestError as item_err: logger.error(f"API error fetching README item for {project.name}/{repo.name}: {item_err}", exc_info=True)
                            except Exception as readme_err: logger.error(f"Error fetching/decoding README for {project.name}/{repo.name}: {readme_err}", exc_info=True)
                        else: logger.debug(f"Skipping README fetch for {project.name}/{repo.name} - no default branch.")

                        # --- Fetch CODEOWNERS Content (Skipped) ---
                        codeowners_content_str = _get_codeowners_content_ado(git_client, project.id, repo.id, default_branch)

                        # --- Fetch Tags (Topics - None for ADO) ---
                        repo_topics = [] # ADO doesn't have direct equivalent

                        # --- Fetch Git Tags ---
                        repo_tags = _fetch_tags_ado(git_client, project.id, repo.id)

                        # --- Get Archived Status (Placeholder) ---
                        # ADO doesn't have a simple repo 'archived' flag. Check project state? Repo disabled?
                        # For now, assume not archived unless project state indicates otherwise.
                        repo_archived = project.state != core_models.ProjectState.WELL_FORMED # Example: Treat non-well-formed projects as potentially archived/inactive

                        repo_data = {
                            # === Core Schema Fields ===
                            "name": repo.name,
                            "description": repo.description or repo.name, # Use description or name
                            "organization": org_name,
                            "repositoryURL": repo.web_url,
                            "homepageURL": repo.web_url, # Default to repo URL
                            "downloadURL": None,
                            "vcs": "git",
                            "repositoryVisibility": repo_visibility,
                            "status": "development", # Placeholder
                            "version": "N/A", # Placeholder
                            "laborHours": 0,
                            "languages": all_languages_list, # Populate with empty list
                            "tags": repo_topics, # Empty list for ADO

                            # === Nested Schema Fields ===
                            "date": {"created": created_at_iso, "lastModified": last_updated_iso},
                            "permissions": {"usageType": None, "exemptionText": None, "licenses": licenses_list},
                            "contact": {"name": "Centers for Disease Control and Prevention", "email": None},
                            "contractNumber": None,

                            # === Fields needed for processing ===
                            "readme_content": readme_content_str,
                            "_codeowners_content": codeowners_content_str, # Will be None
                            "_is_private_flag": repo_visibility == 'private',
                            "_all_languages": all_languages_list,

                            # === Additional Fields ===
                            "repo_id": repo.id,
                            "readme_url": readme_url,
                            "_api_tags": repo_tags, # Store actual Git tags
                            "archived": repo_archived, # Store inferred archived status
                        }

                        processed_data = exemption_processor.process_repository_exemptions(repo_data)

                        # --- Clean up temporary fields ---
                        processed_data.pop('_is_private_flag', None)
                        processed_data.pop('_all_languages', None)
                        processed_data.pop('_api_tags', None)
                        processed_data.pop('archived', None)

                        processed_repo_list.append(processed_data)
                        processed_counter[0] += 1

                    except Exception as repo_proc_err:
                         logger.error(f"Error processing ADO repository '{project.name}/{repo.name}': {repo_proc_err}", exc_info=True)
                         processed_repo_list.append({
                             'name': repo.name,
                             'organization': org_name,
                             'processing_error': f"Connector stage: {repo_proc_err}"
                          })
                         processed_counter[0] += 1

            except AzureDevOpsClientRequestError as proj_repo_err: logger.error(f"ADO API error fetching repos for project '{project.name}': {proj_repo_err}. Skipping.", exc_info=True)
            except Exception as proj_repo_err: logger.error(f"Unexpected error fetching repos for project '{project.name}': {proj_repo_err}. Skipping.", exc_info=True)

        logger.info(f"Finished Azure DevOps scan. Processed {len(processed_repo_list)} repos. Global count: {processed_counter[0]}")

    # --- Exception Handling ---
    except AzureDevOpsClientRequestError as e:
        status_code = getattr(e, 'status_code', None)
        if status_code == 401: logger.error(f"ADO authentication failed (401). Check AZURE_DEVOPS_TOKEN. URL: {organization_url}. Skipping.")
        elif status_code == 404: logger.error(f"ADO organization or base resource not found (404). Check AZURE_DEVOPS_ORG ('{org_name}'). URL: {organization_url}. Skipping.")
        else: logger.error(f"ADO client request error during connection/setup: {e}. Skipping.", exc_info=True)
        return []
    except AzureDevOpsServiceError as e: logger.error(f"ADO service error: {e}. Skipping.", exc_info=True); return []
    except RequestException as e: logger.error(f"Network error connecting to ADO: {e}. Skipping.", exc_info=True); return []
    except Exception as e: logger.error(f"Unexpected error during ADO fetch for org '{org_name}': {e}. Skipping.", exc_info=True); return []

    return processed_repo_list

