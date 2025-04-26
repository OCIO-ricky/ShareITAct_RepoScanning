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
from datetime import datetime
import base64
# --- Import the new processor ---
import exemption_processor

PLACEHOLDER_AZURE_TOKEN = "YOUR_AZURE_DEVOPS_PAT"
PLACEHOLDER_AZURE_ORG = "YourAzureDevOpsOrgName"
PLACEHOLDER_AZURE_PROJECT = "YourAzureDevOpsProjectName"

logger = logging.getLogger(__name__)

def get_azure_devops_org_url():
    """Constructs the Azure DevOps organization URL, returning None if placeholder."""
    org_name = os.getenv("AZURE_DEVOPS_ORG")
    if not org_name or org_name == PLACEHOLDER_AZURE_ORG:
        return None
    base_url = os.getenv("AZURE_DEVOPS_API_URL", "https://dev.azure.com")
    return f"{base_url.rstrip('/')}/{org_name}"

def fetch_repositories(token, org_name, project_name=None) -> list[dict]:
    """
    Fetches repository details from Azure DevOps, processes exemptions,
    and returns a list of processed repository data dictionaries.
    """
    if not token or token == PLACEHOLDER_AZURE_TOKEN:
        logger.info("Azure DevOps token is missing or is a placeholder. Skipping Azure DevOps scan.")
        return []

    organization_url = get_azure_devops_org_url()
    if not organization_url:
         logger.info("Azure DevOps organization name not provided or is placeholder. Skipping Azure DevOps scan.")
         return []

    scan_specific_project = project_name and project_name != PLACEHOLDER_AZURE_PROJECT
    if project_name == PLACEHOLDER_AZURE_PROJECT:
        logger.info("Azure DevOps project name is placeholder, will scan all projects.")

    processed_repo_list = [] # Store final processed data
    connection = None

    try:
        logger.info(f"Attempting to connect to Azure DevOps organization at {organization_url}")
        credentials = BasicAuthentication('', token)
        connection = Connection(base_url=organization_url, creds=credentials)
        core_client = connection.clients.get_core_client()
        git_client = connection.clients.get_git_client()
        logger.info("Azure DevOps SDK connection established.")

        projects_to_scan = []
        if scan_specific_project:
            logger.info(f"Fetching specific project: {project_name}")
            try:
                project_obj = core_client.get_project(project_name)
                projects_to_scan = [project_obj]
            except AzureDevOpsClientRequestError as proj_404_err:
                 status_code = getattr(proj_404_err, 'status_code', None)
                 if status_code == 404:
                      logger.error(f"Azure DevOps specific project '{project_name}' not found (404). Skipping.")
                 else:
                      logger.error(f"Azure DevOps error fetching specific project '{project_name}': {proj_404_err}. Skipping.", exc_info=True)
                 return []
            except Exception as proj_err:
                 logger.error(f"Unexpected error fetching specific Azure DevOps project '{project_name}': {proj_err}. Skipping.", exc_info=True)
                 return []
        else:
            logger.info("Fetching all projects in the organization...")
            projects_to_scan = core_client.get_projects()

        if not projects_to_scan:
             logger.warning(f"No Azure DevOps projects found for organization '{org_name}'" + (f" matching name '{project_name}'" if scan_specific_project else "."))
             return []

        total_repos_found = 0
        for project in projects_to_scan:
            project_repo_count = 0
            logger.debug(f"Fetching repositories for project: {project.name}...")
            try:
                project_repos = git_client.get_repositories(project.id)
                for i, repo in enumerate(project_repos):
                    project_repo_count = i + 1
                    total_repos_found += 1
                    repo_data = {} # Start fresh for each repo
                    try:
                        logger.debug(f"Fetching data for ADO repo: {project.name}/{repo.name}")

                        # --- Fetch Base Data ---
                        default_branch = repo.default_branch.replace('refs/heads/', '') if repo.default_branch else None

                        repo_data = {
                            "source": "AzureDevOps",
                            "id": repo.id, # Use repo ID
                            "org_name": org_name, # Use the overall org name
                            "project_name": project.name, # Keep project name separate if needed
                            "repo_name": repo.name, # Just the repo name
                            "full_name": f"{project.name}/{repo.name}", # Combine project/repo name
                            "description": repo.name, # No separate description field easily available
                            "html_url": repo.web_url,
                            "api_url": repo.url,
                            "is_private": project.visibility != core_models.ProjectVisibility.PUBLIC,
                            "is_fork": repo.is_fork,
                            "created_at": None, # Not easily available
                            "updated_at": None, # Will fetch last commit date
                            "pushed_at": None, # Will fetch last commit date
                            "last_updated": None, # Will fetch last commit date
                            "default_branch": default_branch,
                            "language": None, # Placeholder - ADO doesn't provide easily
                            "license_name": None, # Placeholder
                            "readme_url": None,
                            "readme_content": None, # Will be fetched next
                            "contact_email": None,
                            # Exemption fields added by processor
                        }

                        # --- Fetch Last Commit (for last_updated) ---
                        last_updated_iso = None
                        if default_branch: # Need branch to get commits usually
                            try:
                                commits = git_client.get_commits(
                                    repository_id=repo.id,
                                    project=project.id,
                                    search_criteria=git_models.GitQueryCommitsCriteria(
                                        top=1,
                                        item_version=git_models.GitVersionDescriptor(
                                            version=default_branch,
                                            version_type=git_models.GitVersionType.BRANCH
                                        )
                                    )
                                )
                                if commits:
                                    committer_date = commits[0].committer.date
                                    if isinstance(committer_date, datetime):
                                        last_updated_iso = committer_date.isoformat()
                                    elif isinstance(committer_date, str):
                                        try:
                                            # Handle potential timezone formats
                                            dt_obj = datetime.fromisoformat(committer_date.replace('Z', '+00:00'))
                                            last_updated_iso = dt_obj.isoformat()
                                        except ValueError:
                                            logger.warning(f"Could not parse commit date string '{committer_date}' for {project.name}/{repo.name}")
                                repo_data['updated_at'] = last_updated_iso
                                repo_data['pushed_at'] = last_updated_iso
                                repo_data['last_updated'] = last_updated_iso
                            except Exception as commit_err:
                                logger.error(f"Error fetching last commit for {project.name}/{repo.name}: {commit_err}", exc_info=True)
                        else:
                            logger.debug(f"Skipping last commit fetch for {project.name}/{repo.name} - no default branch.")


                        # --- Fetch README Content ---
                        if default_branch:
                            try:
                                # Look for common README filenames
                                for readme_path in ["README.md", "README.txt", "README"]:
                                    try:
                                        item = git_client.get_item(
                                            repository_id=repo.id,
                                            project=project.id,
                                            path=f"/{readme_path}",
                                            version_descriptor=git_models.GitVersionDescriptor(
                                                version=default_branch,
                                                version_type=git_models.GitVersionType.BRANCH
                                            ),
                                            include_content=True
                                        )
                                        if item and item.content:
                                            try:
                                                readme_content_bytes = base64.b64decode(item.content)
                                                try:
                                                    repo_data['readme_content'] = readme_content_bytes.decode('utf-8')
                                                except UnicodeDecodeError:
                                                    repo_data['readme_content'] = readme_content_bytes.decode('latin-1')
                                            except Exception: # Fallback if not base64
                                                repo_data['readme_content'] = item.content
                                            repo_data['readme_url'] = f"{repo.web_url}?path=/{readme_path}&version=GB{default_branch}&_a=contents"
                                            logger.debug(f"Fetched README '{readme_path}' for {project.name}/{repo.name}")
                                            break # Found one, stop looking
                                    except AzureDevOpsClientRequestError as item_404_err:
                                        status_code = getattr(item_404_err, 'status_code', None)
                                        if status_code == 404:
                                            continue # Try next readme filename
                                        else:
                                            raise # Re-raise other API errors
                            except AzureDevOpsClientRequestError as item_err:
                                 logger.error(f"API error fetching README item for {project.name}/{repo.name}: {item_err}", exc_info=True)
                            except Exception as readme_err:
                                logger.error(f"Error fetching/decoding README for {project.name}/{repo.name}: {readme_err}", exc_info=True)
                        else:
                            logger.debug(f"Skipping README fetch for {project.name}/{repo.name} - no default branch.")


                        # --- Call Exemption Processor ---
                        processed_data = exemption_processor.process_repository_exemptions(repo_data)

                        # --- Clean up ---
                        processed_data.pop('readme_content', None)

                        # Add the fully processed data
                        processed_repo_list.append(processed_data)

                    except Exception as repo_proc_err:
                         logger.error(f"Error processing ADO repository '{project.name}/{repo.name}': {repo_proc_err}", exc_info=True)
                         # Optionally append minimal error info
                         # processed_repo_list.append({'repo_name': f"{project.name}/{repo.name}", 'error': str(repo_proc_err)})

            except AzureDevOpsClientRequestError as proj_repo_err:
                 logger.error(f"Azure DevOps API error fetching repositories for project '{project.name}': {proj_repo_err}. Skipping project.", exc_info=True)
            except Exception as proj_repo_err:
                 logger.error(f"Unexpected error fetching repositories for project '{project.name}': {proj_repo_err}. Skipping project.", exc_info=True)

        logger.info(f"Successfully fetched and processed {len(processed_repo_list)} total repositories from Azure DevOps.")

    except AzureDevOpsClientRequestError as e:
        status_code = getattr(e, 'status_code', None)
        if status_code == 401:
             logger.error(f"Azure DevOps authentication failed (401). Check AZURE_DEVOPS_TOKEN. URL: {organization_url}. Skipping.")
        elif status_code == 404:
             logger.error(f"Azure DevOps organization or base resource not found (404). Check AZURE_DEVOPS_ORG ('{org_name}'). URL: {organization_url}. Skipping.")
        else:
             logger.error(f"Azure DevOps client request error during connection/setup: {e}. Skipping.", exc_info=True)
        return []
    except AzureDevOpsServiceError as e:
        logger.error(f"Azure DevOps service error: {e}. Skipping.", exc_info=True)
        return []
    except RequestException as e:
        logger.error(f"Network error connecting to Azure DevOps: {e}. Skipping.", exc_info=True)
        return []
    except Exception as e:
        logger.error(f"Unexpected error during Azure DevOps fetch for org '{org_name}': {e}. Skipping.", exc_info=True)
        return []

    return processed_repo_list
