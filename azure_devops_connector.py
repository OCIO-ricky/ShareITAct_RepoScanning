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
import exemption_processor # Ensure this import is present

# DO NOT CHANGE THESE PLACEHOLDERS !!!
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

# --- UPDATE function signature ---
def fetch_repositories(token, org_name, project_name, processed_counter: list[int], debug_limit: int | None) -> list[dict]:
    """
    Fetches repository details from Azure DevOps, processes exemptions,
    respecting a global limit, and returns a list of processed repository data dictionaries.

    Args:
        token: Azure DevOps PAT.
        org_name: Azure DevOps organization name.
        project_name: Specific Azure DevOps project name (or None/placeholder to scan all).
        processed_counter: A mutable list containing the current global count of processed repos.
        debug_limit: The maximum number of repos to process globally (or None).
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
                 return [] # Exit if specific project not found or error occurs
            except Exception as proj_err:
                 logger.error(f"Unexpected error fetching specific Azure DevOps project '{project_name}': {proj_err}. Skipping.", exc_info=True)
                 return [] # Exit on other errors fetching specific project
        else: 
            logger.info("Fetching all projects in the organization...")
            projects_to_scan = core_client.get_projects()

        if not projects_to_scan:
             logger.warning(f"No Azure DevOps projects found for organization '{org_name}'" + (f" matching name '{project_name}'" if scan_specific_project else "."))
             return []

        # --- Loop through projects ---
        for project in projects_to_scan:
            # --- ADD DEBUG CHECK (Project Level) ---
            # Check limit before even fetching repos for this project
            if debug_limit is not None and processed_counter[0] >= debug_limit:
                logger.warning(f"--- DEBUG MODE: Global limit ({debug_limit}) reached before processing project '{project.name}'. Skipping project. ---")
                continue # Skip to the next project (or effectively stop if this was the last one)
            # --- END DEBUG CHECK ---

            logger.debug(f"Fetching repositories for project: {project.name}...")
            try:
                project_repos = git_client.get_repositories(project.id)
                # --- Loop through repos, respecting the limit ---
                for i, repo in enumerate(project_repos):
                    # --- ADD DEBUG CHECK (Repo Level) ---
                    if debug_limit is not None and processed_counter[0] >= debug_limit:
                        logger.warning(f"--- DEBUG MODE: Global limit ({debug_limit}) reached during ADO scan within project '{project.name}'. Stopping ADO fetch for this project. ---")
                        break # Exit the loop over repos within this project
                    # --- END DEBUG CHECK ---

                    repo_data = {} # Start fresh for each repo
                    try:
                        # --- Add fork check early ---
                        if repo.is_fork:
                            logger.info(f"Skipping forked repository: {project.name}/{repo.name}")
                            continue
                        # --- End fork check ---

                        logger.debug(f"Fetching data for ADO repo: {project.name}/{repo.name}")

                        # --- Fetch Base Data ---
                        default_branch = repo.default_branch.replace('refs/heads/', '') if repo.default_branch else None
                        repo_visibility = "private" if project.visibility != core_models.ProjectVisibility.PUBLIC else "public"
                        # ADO doesn't provide created_at for repo easily, default to None
                        created_at_iso = None
                        # Language and License are not readily available in ADO API
                        repo_language = None
                        licenses_list = []

                        # --- Fetch Last Commit Date ---
                        last_updated_iso = None
                        if default_branch:
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
                                            # Attempt parsing ISO format, handling potential 'Z'
                                            dt_obj = datetime.fromisoformat(committer_date.replace('Z', '+00:00'))
                                            last_updated_iso = dt_obj.isoformat()
                                        except ValueError:
                                            logger.warning(f"Could not parse commit date string '{committer_date}' for {project.name}/{repo.name}")
                            except Exception as commit_err:
                                logger.error(f"Error fetching last commit for {project.name}/{repo.name}: {commit_err}", exc_info=True)
                        else:
                            logger.debug(f"Skipping last commit fetch for {project.name}/{repo.name} - no default branch.")

                        # --- ADD DEFAULT LICENSE ---
                        if not licenses_list:
                            logger.debug(f"No license found via API for {project.name}/{repo.name}. Applying default: Apache License 2.0")
                            licenses_list.append({
                                "name": "Apache License 2.0",
                                "URL": "https://www.apache.org/licenses/LICENSE-2.0"
                            })
                        # --- END DEFAULT LICENSE ---

                        # Build the dictionary using schema 2.0 field names where possible
                        repo_data = {
                            # === Core Schema Fields ===
                            "name": repo.name,
                            # Use repo name as description if none available
                            "description": repo.name,
                            # Use the overall org name passed to the function
                            "organization": org_name,
                            "repositoryURL": repo.web_url,
                            "homepageURL": repo.web_url, # Default to repo URL
                            "downloadURL": None, # Requires release info
                            "vcs": "git",
                            "repositoryVisibility": repo_visibility,
                            "status": "development", # Placeholder
                            "version": "N/A", # Placeholder
                            "laborHours": 0, # Placeholder
                            "languages": [], # ADO doesn't provide easily
                            "tags": [], # Placeholder - requires fetching tags API

                            # === Nested Schema Fields ===
                            "date": {
                                "created": created_at_iso, # Often None for ADO
                                "lastModified": last_updated_iso, # From last commit
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
                            "_language_heuristic": repo_language, # Temp field (will be None)

                            # === Additional Fields (Kept at the end) / Doesn't hurt ===
                            "repo_id": repo.id,
                            "readme_url": None, # Will be updated after fetching README
                        }


                        # --- Fetch README.MD Content ---
                        if default_branch:
                            try:
                                # Look for common README filenames
                                readme_found = False
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
                                                # ADO content might not be base64, try direct first
                                                repo_data['readme_content'] = item.content
                                            except Exception: # Fallback to base64 decode if direct fails
                                                try:
                                                    readme_content_bytes = base64.b64decode(item.content)
                                                    try:
                                                        repo_data['readme_content'] = readme_content_bytes.decode('utf-8')
                                                    except UnicodeDecodeError:
                                                        repo_data['readme_content'] = readme_content_bytes.decode('latin-1')
                                                except Exception as decode_err:
                                                     logger.warning(f"Could not decode README content for {project.name}/{repo.name}: {decode_err}")
                                                     repo_data['readme_content'] = None # Ensure it's None if decode fails

                                            if repo_data['readme_content']: # Only set URL if content was obtained
                                                # Construct a likely README URL
                                                repo_data['readme_url'] = f"{repo.web_url}?path=/{readme_path}&version=GB{default_branch}&_a=contents"
                                                logger.debug(f"Fetched README '{readme_path}' for {project.name}/{repo.name}")
                                                readme_found = True
                                                break # Found one, stop looking
                                    except AzureDevOpsClientRequestError as item_404_err:
                                        status_code = getattr(item_404_err, 'status_code', None)
                                        if status_code == 404:
                                            continue # Try next readme filename
                                        else:
                                            raise # Re-raise other API errors
                                if not readme_found:
                                     logger.debug(f"No common README file found for {project.name}/{repo.name}")
                            except AzureDevOpsClientRequestError as item_err:
                                 logger.error(f"API error fetching README item for {project.name}/{repo.name}: {item_err}", exc_info=True)
                            except Exception as readme_err:
                                logger.error(f"Error fetching/decoding README for {project.name}/{repo.name}: {readme_err}", exc_info=True)
                        else:
                            logger.debug(f"Skipping README fetch for {project.name}/{repo.name} - no default branch.")


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

                    except Exception as repo_proc_err:
                         logger.error(f"Error processing ADO repository '{project.name}/{repo.name}': {repo_proc_err}", exc_info=True)
                         # Do NOT increment counter if an error occurred

            except AzureDevOpsClientRequestError as proj_repo_err:
                 logger.error(f"Azure DevOps API error fetching repositories for project '{project.name}': {proj_repo_err}. Skipping project.", exc_info=True)
            except Exception as proj_repo_err:
                 logger.error(f"Unexpected error fetching repositories for project '{project.name}': {proj_repo_err}. Skipping project.", exc_info=True)

        # --- UPDATE Log Message ---
        logger.info(f"Finished Azure DevOps scan. Processed {len(processed_repo_list)} repositories in this connector. Global count: {processed_counter[0]}")

    # --- Exception Handling (as before) ---
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
