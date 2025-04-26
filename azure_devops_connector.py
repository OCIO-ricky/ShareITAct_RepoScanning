# azure_devops_connector.py
import logging
import os
from azure.devops.connection import Connection
from msrest.authentication import BasicAuthentication
# Import specific exceptions
from azure.devops.exceptions import AzureDevOpsClientRequestError, AzureDevOpsServiceError
from azure.devops.v7_0.core import CoreClient
from azure.devops.v7_0.core import models as core_models
from azure.devops.v7_0.git import GitClient
from requests.exceptions import RequestException # Assuming requests is used
from datetime import datetime # Import datetime

# Define placeholder values
PLACEHOLDER_AZURE_TOKEN = "YOUR_AZURE_DEVOPS_PAT"
PLACEHOLDER_AZURE_ORG = "YourAzureDevOpsOrgName"
PLACEHOLDER_AZURE_PROJECT = "YourAzureDevOpsProjectName"

logger = logging.getLogger(__name__)

def get_azure_devops_org_url():
    """Constructs the Azure DevOps organization URL, returning None if placeholder."""
    org_name = os.getenv("AZURE_DEVOPS_ORG")
    if not org_name or org_name == PLACEHOLDER_AZURE_ORG:
        return None # Signal that org is placeholder/missing
    base_url = os.getenv("AZURE_DEVOPS_API_URL", "https://dev.azure.com")
    return f"{base_url.rstrip('/')}/{org_name}"

def fetch_repositories(token, org_name, project_name=None):
    """Fetches repository details from Azure DevOps organization."""
    # --- Check for placeholder token ---
    if not token or token == PLACEHOLDER_AZURE_TOKEN:
        logger.info("Azure DevOps token is missing or is a placeholder. Skipping Azure DevOps scan.")
        return []
    # --- End Check ---

    organization_url = get_azure_devops_org_url()
    if not organization_url:
         logger.info("Azure DevOps organization name not provided or is placeholder. Skipping Azure DevOps scan.")
         return []

    # Check project placeholder here too
    scan_specific_project = project_name and project_name != PLACEHOLDER_AZURE_PROJECT
    if project_name == PLACEHOLDER_AZURE_PROJECT:
        logger.info("Azure DevOps project name is placeholder, will scan all projects.")


    repositories = []
    connection = None

    try:
        # --- SDK Connection ---
        logger.info(f"Attempting to connect to Azure DevOps organization at {organization_url}")
        credentials = BasicAuthentication('', token)
        connection = Connection(base_url=organization_url, creds=credentials)
        # Verify connection by getting core client - this often triggers auth/404 checks
        core_client = connection.clients.get_core_client()
        git_client = connection.clients.get_git_client()
        logger.info("Azure DevOps SDK connection established.")
        # --- End SDK Connection ---

        # Get projects
        projects_to_scan = []
        if scan_specific_project:
            logger.info(f"Fetching specific project: {project_name}")
            # Wrap get_project in try-except for specific project 404
            try:
                project_obj = core_client.get_project(project_name)
                projects_to_scan = [project_obj]
            except AzureDevOpsClientRequestError as proj_404_err:
                 status_code = getattr(proj_404_err, 'status_code', None)
                 if status_code == 404:
                      logger.error(f"Azure DevOps specific project '{project_name}' not found (404). Skipping Azure DevOps scan.")
                 else:
                      logger.error(f"Azure DevOps error fetching specific project '{project_name}': {proj_404_err}. Skipping Azure DevOps scan.", exc_info=True)
                 return [] # Stop if specific project not found
            except Exception as proj_err: # Catch other errors fetching specific project
                 logger.error(f"Unexpected error fetching specific Azure DevOps project '{project_name}': {proj_err}. Skipping Azure DevOps scan.", exc_info=True)
                 return []
        else:
            logger.info("Fetching all projects in the organization...")
            projects_to_scan = core_client.get_projects()


        if not projects_to_scan:
             logger.warning(f"No Azure DevOps projects found for organization '{org_name}'" + (f" matching name '{project_name}'" if scan_specific_project else "."))
             return []

        total_repos_found = 0
        log_interval = 50 # Log progress interval

        for project in projects_to_scan:
            project_repo_count = 0
            logger.debug(f"Fetching repositories for project: {project.name}...")
            try:
                project_repos = git_client.get_repositories(project.id)
                for i, repo in enumerate(project_repos):
                    project_repo_count = i + 1
                    total_repos_found += 1

                    # Basic repo info - Adapt to common format
                    # Note: last_updated requires extra calls
                    last_updated_iso = None # Placeholder

                    repo_info = {
                        "source": "AzureDevOps",
                        "org_name": org_name,
                        "repo_name": f"{project.name}/{repo.name}", # Combine project/repo name
                        "description": repo.name, # ADO repo object doesn't have a separate description field easily accessible here
                        "html_url": repo.web_url,
                        "api_url": repo.url, # API URL for the repo
                        "is_private": project.visibility != core_models.ProjectVisibility.PUBLIC, # Check project visibility
                        "is_fork": repo.is_fork,
                        "last_updated": last_updated_iso, # Requires fetching commit data
                        "default_branch": repo.default_branch.replace('refs/heads/', '') if repo.default_branch else None,
                        "language": None, # Requires analyzing content or specific API calls
                        "license_name": None, # Requires fetching license file content
                        "readme_url": None, # Requires fetching README file details
                        "contact_email": None, # Placeholder
                        "exempted": False, # Placeholder
                        "exemption_reason": None # Placeholder
                    }
                    repositories.append(repo_info)

                logger.debug(f"Fetched {project_repo_count} repositories from project '{project.name}'.")

            except AzureDevOpsClientRequestError as proj_repo_err:
                 # Handle errors fetching repos for a specific project (e.g., permissions)
                 logger.error(f"Azure DevOps API error fetching repositories for project '{project.name}': {proj_repo_err}. Skipping project.", exc_info=True)
                 # Continue to the next project
            except Exception as proj_repo_err:
                 logger.error(f"Unexpected error fetching repositories for project '{project.name}': {proj_repo_err}. Skipping project.", exc_info=True)
                 # Continue to the next project

        logger.info(f"Successfully fetched details for {len(repositories)} total repositories from Azure DevOps.")

    # --- Specific Exception Handling for Connection/Initialization ---
    except AzureDevOpsClientRequestError as e:
        status_code = getattr(e, 'status_code', None)
        if status_code == 401:
             logger.error(f"Azure DevOps authentication failed (401 Unauthorized). Check your AZURE_DEVOPS_TOKEN. URL: {organization_url}. Skipping Azure DevOps scan.")
        elif status_code == 404:
             # This might happen if the org URL itself is wrong during initial connection
             logger.error(f"Azure DevOps organization or base resource not found (404 Not Found). Check AZURE_DEVOPS_ORG ('{org_name}'). URL: {organization_url}. Skipping Azure DevOps scan.")
        else:
             logger.error(f"Azure DevOps client request error during connection/setup: {e}. Skipping Azure DevOps scan.", exc_info=True)
        return []
    except AzureDevOpsServiceError as e: # Catch broader service errors
        logger.error(f"Azure DevOps service error: {e}. Skipping Azure DevOps scan.", exc_info=True)
        return []
    except RequestException as e: # Catch network errors
        logger.error(f"A network error occurred connecting to Azure DevOps: {e}. Skipping Azure DevOps scan.", exc_info=True)
        return []
    except Exception as e: # Catch any other unexpected errors
        logger.error(f"An unexpected error occurred during Azure DevOps fetch for org '{org_name}': {e}. Skipping Azure DevOps scan.", exc_info=True)
        return []

    return repositories


