# clients/azure_devops_connector.py
"""
Azure DevOps Connector for Share IT Act Repository Scanning Tool.

This module is responsible for fetching repository data from Azure DevOps,
including metadata, README content, and other relevant details.
It interacts with the Azure DevOps REST API via the azure-devops Python SDK.
"""

import os
import logging
import base64
from typing import List, Dict, Optional, Any, Tuple
from datetime import timezone, datetime
from utils.labor_hrs_estimator import _create_summary_dataframe # Import the labor hrs estimator


# --- Try importing Azure DevOps SDK ---
try:
    from azure.devops.connection import Connection
    from msrest.authentication import BasicAuthentication, ServicePrincipalCredentials
    from azure.devops.v7_1.git import GitClient
    from azure.devops.v7_1.core import CoreClient
    from azure.devops.v7_1.git.models import GitRepository
    from azure.devops.exceptions import AzureDevOpsServiceError
    AZURE_SDK_AVAILABLE = True
except ImportError as e:
    AZURE_SDK_AVAILABLE = False
    GitClient = type('GitClient', (object,), {})
    CoreClient = type('CoreClient', (object,), {})
    AzureDevOpsServiceError = type('AzureDevOpsServiceError', (Exception,), {})
    Connection = type('Connection', (object,), {})
    BasicAuthentication = type('BasicAuthentication', (object,), {})
    ServicePrincipalCredentials = type('ServicePrincipalCredentials', (object,), {}) # Add dummy for SPN
    # Log which specific import failed if possible
    logging.getLogger(__name__).warning(
       f"Failed to import a component required for Azure DevOps SDK. Full error: {e}. "
         "Azure DevOps scanning will be skipped. Install with: pip install azure-devops"
    )
# Attempt to import the exemption processor
try:
    from utils import exemption_processor
except ImportError:
    logging.getLogger(__name__).error(
        "Failed to import exemption_processor from utils. "
        "Exemption processing will be skipped by the Azure DevOps connector (using mock)."
    )
    class MockExemptionProcessor:
        def process_repository_exemptions(self, repo_data: Dict[str, Any], default_org_identifiers: Optional[List[str]] = None) -> Dict[str, Any]:
            repo_data.setdefault('_status_from_readme', None)
            repo_data.setdefault('_private_contact_emails', [])
            repo_data.setdefault('contact', {})
            repo_data.setdefault('permissions', {"usageType": "openSource"})
            repo_data.pop('readme_content', None)
            repo_data.pop('_codeowners_content', None)
            return repo_data
    exemption_processor = MockExemptionProcessor()

# load_dotenv() # No longer loading .env directly for auth in this connector
logger = logging.getLogger(__name__)

PLACEHOLDER_AZURE_TOKEN = "YOUR_AZURE_DEVOPS_PAT"
PLACEHOLDER_AZURE_CLIENT_ID = "YOUR_AZURE_CLIENT_ID"
PLACEHOLDER_AZURE_CLIENT_SECRET = "YOUR_AZURE_CLIENT_SECRET"
PLACEHOLDER_AZURE_TENANT_ID = "YOUR_AZURE_TENANT_ID"

AZURE_DEVOPS_RESOURCE_ID = "499b84ac-1321-427f-aa17-267ca6975798" # Static Azure DevOps resource ID

def is_placeholder_token(token: Optional[str]) -> bool:
    """Checks if the Azure DevOps PAT is missing or a known placeholder."""
    return not token or token == PLACEHOLDER_AZURE_TOKEN

def are_spn_details_placeholders(client_id: Optional[str], client_secret: Optional[str], tenant_id: Optional[str]) -> bool:
    """Checks if any SPN detail is missing or a known placeholder."""
    return not client_id or client_id == PLACEHOLDER_AZURE_CLIENT_ID or \
           not client_secret or client_secret == PLACEHOLDER_AZURE_CLIENT_SECRET or \
           not tenant_id or tenant_id == PLACEHOLDER_AZURE_TENANT_ID


def _get_file_content_azure(git_client: GitClient, repository_id: str, project_name: str, file_path: str, repo_default_branch: Optional[str]) -> Optional[str]:
    if not AZURE_SDK_AVAILABLE: return None
    if not repo_default_branch:
        logger.warning(f"Cannot fetch file '{file_path}' for repo ID {repository_id} in {project_name}: No default branch identified.")
        return None
    try:
        normalized_file_path = file_path.lstrip('/')
        item_content_stream = git_client.get_item_text(
            repository_id=repository_id,
            path=normalized_file_path,
            project=project_name,
            download=True,
            version_descriptor={'version': repo_default_branch}
        )
        content_str = ""
        for chunk in item_content_stream:
            content_str += chunk.decode('utf-8', errors='replace')
        return content_str
    except AzureDevOpsServiceError as e:
        if "TF401019" in str(e) or "Item not found" in str(e) or (hasattr(e, 'status_code') and e.status_code == 404):
            logger.debug(f"File '{file_path}' not found in repo ID {repository_id} (project: {project_name}). Error: {e}")
        else:
            logger.error(f"Azure DevOps API error fetching file '{file_path}' for repo ID {repository_id}: {e}", exc_info=False)
    except Exception as e:
        logger.error(f"Unexpected error fetching file '{file_path}' for repo ID {repository_id}: {e}", exc_info=True)
    return None


def _get_readme_details_azure(git_client: GitClient, repository_id: str, project_name: str, repo_web_url: str, repo_default_branch: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    common_readme_names = ["README.md", "README.txt", "README"]
    if not repo_default_branch:
        logger.warning(f"Cannot fetch README for repo ID {repository_id} in {project_name}: No default branch identified.")
        return None, None

    for readme_name in common_readme_names:
        content = _get_file_content_azure(git_client, repository_id, project_name, readme_name, repo_default_branch)
        if content:
            url_readme_name = readme_name.lstrip('/')
            branch_name_for_url = repo_default_branch.replace('refs/heads/', '')
            readme_url = f"{repo_web_url}?path=/{url_readme_name}&version=GB{branch_name_for_url}&_a=contents"
            logger.debug(f"Successfully fetched README '{readme_name}' for repo ID {repository_id}")
            return content, readme_url
    logger.debug(f"No common README file found for repo ID {repository_id}")
    return None, None

def _get_codeowners_content_azure(git_client: GitClient, repository_id: str, project_name: str, repo_default_branch: Optional[str]) -> Optional[str]:
    codeowners_locations = ["CODEOWNERS", ".azuredevops/CODEOWNERS", "docs/CODEOWNERS", ".vsts/CODEOWNERS"]
    if not repo_default_branch:
        logger.warning(f"Cannot fetch CODEOWNERS for repo ID {repository_id} in {project_name}: No default branch identified.")
        return None

    for location in codeowners_locations:
        normalized_location = location.lstrip('/')
        content = _get_file_content_azure(git_client, repository_id, project_name, normalized_location, repo_default_branch)
        if content:
            logger.debug(f"Successfully fetched CODEOWNERS from '{location}' for repo ID {repository_id}")
            return content
    logger.debug(f"No CODEOWNERS file found in standard locations for repo ID {repository_id}")
    return None

def _fetch_tags_azure(git_client: GitClient, repository_id: str, project_name: str) -> List[str]:
    if not AZURE_SDK_AVAILABLE: return []
    tag_names = []
    try:
        logger.debug(f"Fetching tags for repo ID: {repository_id} in project {project_name}")
        refs = git_client.get_refs(repository_id=repository_id, project=project_name, filter="tags/")
        for ref in refs:
            if ref.name and ref.name.startswith("refs/tags/"):
                tag_names.append(ref.name.replace("refs/tags/", ""))
        logger.debug(f"Found {len(tag_names)} tags for repo ID {repository_id}")
    except AzureDevOpsServiceError as e:
        logger.error(f"Azure DevOps API error fetching tags for repo ID {repository_id}: {e}", exc_info=False)
    except Exception as e:
        logger.error(f"Unexpected error fetching tags for repo ID {repository_id}: {e}", exc_info=True)
    return tag_names


def fetch_repositories(
    pat_token: Optional[str],
    spn_client_id: Optional[str],
    spn_client_secret: Optional[str],
    spn_tenant_id: Optional[str],
    organization_name: str, 
    project_name: str, 
    processed_counter: List[int], 
    debug_limit: Optional[int] = None,
    hours_per_commit: Optional[float] = None) -> list[dict]:
    """
    Fetches repository details from a specific Azure DevOps project.
    Uses Service Principal if all SPN details are provided, otherwise falls back to PAT.
    """
    logger.info(f"Attempting to fetch repositories for Azure DevOps project: {organization_name}/{project_name}")
    if not AZURE_SDK_AVAILABLE:
        logger.error("Azure DevOps SDK not available. Skipping Azure DevOps scan.")
        return []

    # Get the base URL for the Azure DevOps API from environment (still useful for non-auth config)
    # Or allow it to be passed if you want to make it fully CLI driven in generate_codejson.py
    azure_devops_api_url = os.getenv("AZURE_DEVOPS_API_URL", "https://dev.azure.com").strip('/')
    organization_url = f"{azure_devops_api_url}/{organization_name}"

    processed_repo_list: List[Dict[str, Any]] = []
    credentials = None
    auth_method = ""

    try:
        # Prioritize Service Principal if all details are provided and not placeholders
        if not are_spn_details_placeholders(spn_client_id, spn_client_secret, spn_tenant_id):
            logger.info("Attempting Azure DevOps authentication using Service Principal.")
            if not ServicePrincipalCredentials: # Should not happen if AZURE_SDK_AVAILABLE is True and imports worked
                logger.error("ServicePrincipalCredentials class not available. Cannot use SPN auth.")
                return []
            credentials = ServicePrincipalCredentials(
                client=spn_client_id,
                secret=spn_client_secret,
                tenant=spn_tenant_id,
                resource=AZURE_DEVOPS_RESOURCE_ID
            )
            auth_method = "Service Principal"
        # Fallback to PAT if SPN details are not complete/valid
        elif not is_placeholder_token(pat_token):
            logger.info("Attempting Azure DevOps authentication using Personal Access Token (PAT).")
            if not BasicAuthentication: # Should not happen
                logger.error("BasicAuthentication class not available. Cannot use PAT auth.")
                return []
            credentials = BasicAuthentication('', pat_token) 
            auth_method = "PAT"
        else:
            logger.error("Azure DevOps authentication failed: Neither valid Service Principal details nor a PAT were provided via CLI, or they are placeholders.")
            return []

        connection = Connection(base_url=organization_url, creds=credentials)
        git_client: GitClient = connection.clients.get_git_client()
        core_client: CoreClient = connection.clients.get_core_client()

        logger.info(f"Successfully established connection to Azure DevOps organization: {organization_name} using {auth_method}.")

        repositories: List[GitRepository] = git_client.get_repositories(project=project_name)
        repo_count_for_project = 0

        for repo in repositories:
            if debug_limit is not None and processed_counter[0] >= debug_limit:
                logger.info(f"Global debug limit ({debug_limit}) reached. Stopping repository fetching for {organization_name}/{project_name}.")
                break

            repo_full_name = f"{organization_name}/{project_name}/{repo.name}"
            logger.info(f"Processing repository: {repo_full_name}")
            repo_count_for_project += 1
            
            repo_data: Dict[str, Any] = {}
            try:
                if repo.is_fork and repo.parent_repository:
                    parent_info = "unknown parent"
                    if repo.parent_repository.name and repo.parent_repository.project and repo.parent_repository.project.name:
                        parent_info = f"{repo.parent_repository.project.name}/{repo.parent_repository.name}"
                    elif repo.parent_repository.name:
                        parent_info = repo.parent_repository.name
                    logger.info(f"Skipping forked repository: {repo.name} (fork of {parent_info})")
                    continue

                created_at_iso: Optional[str] = None 
                pushed_at_iso: Optional[str] = None
                if repo.project and repo.project.last_update_time:
                    last_modified_dt = repo.project.last_update_time.replace(tzinfo=timezone.utc)
                    pushed_at_iso = last_modified_dt.isoformat()
                
                repo_visibility = "private" 
                try:
                    project_details = core_client.get_project(project_id=project_name)
                    if project_details and project_details.visibility:
                        vis = project_details.visibility.lower()
                        if vis in ["public", "private"]:
                            repo_visibility = vis
                        else:
                            logger.warning(f"Unexpected project visibility '{vis}' for {repo_full_name}. Defaulting to 'private'.")
                except Exception as proj_vis_err:
                    logger.warning(f"Could not determine project visibility for {repo_full_name}: {proj_vis_err}. Defaulting to 'private'.")

                all_languages_list = [] 
                repo_topics = []      

                repo_default_branch = repo.default_branch 
                readme_content_str, readme_html_url = _get_readme_details_azure(git_client, repo.id, project_name, repo.web_url, repo_default_branch)
                codeowners_content_str = _get_codeowners_content_azure(git_client, repo.id, project_name, repo_default_branch)
                
                licenses_list = [] 
                repo_git_tags = _fetch_tags_azure(git_client, repo.id, project_name)

                repo_data = {
                    "name": repo.name,
                    "organization": organization_name, 
                    "description": repo.project.description if repo.project and repo.project.description else "",
                    "repositoryURL": repo.web_url,
                    "homepageURL": repo.web_url,
                    "downloadURL": None,
                    "vcs": "git",
                    "repositoryVisibility": repo_visibility,
                    "status": "development",
                    "version": "N/A",
                    "laborHours": 0,
                    "languages": all_languages_list,
                    "tags": repo_topics,
                    "date": {
                        "created": created_at_iso,
                        "lastModified": pushed_at_iso,
                    },
                    "permissions": {
                        "usageType": "openSource",
                        "exemptionText": None,
                        "licenses": licenses_list
                    },
                    "contact": {},
                    "contractNumber": None,
                    "readme_content": readme_content_str,
                    "_codeowners_content": codeowners_content_str,
                    "repo_id": repo.id,
                    "readme_url": readme_html_url,
                    "_api_tags": repo_git_tags,
                    "archived": repo.is_disabled if hasattr(repo, 'is_disabled') else False,
                    "_azure_project_name": project_name
                }

                default_ids_for_exemption = [organization_name]
                if project_name and project_name.lower() != organization_name.lower():
                    default_ids_for_exemption.append(project_name)

                if hours_per_commit is not None:
                    logger.debug(f"Estimating labor hours for Azure DevOps repo: {repo.name} in {project_name}")
                    try:
                        commit_details_list = []
                        # Fetch commits using the Azure DevOps SDK's GitClient
                        # Note: get_commits might return a limited number by default.
                        # For a full history, we might need to handle pagination or use a large 'top'.
                        # The SDK's get_commits method might have a default limit (e.g., 100).
                        # For simplicity here, fetching up to 10000. Adjust as needed.
                        # A more robust solution would implement paging if repo has more commits.
                        ado_commits = git_client.get_commits(repository_id=repo.id, project=project_name, top=10000)

                        for ado_commit in ado_commits:
                            if ado_commit.author and ado_commit.author.name and ado_commit.author.email and ado_commit.author.date:
                                commit_details_list.append((
                                    ado_commit.author.name,
                                    ado_commit.author.email,
                                    ado_commit.author.date # This is already a datetime object
                                ))
                        
                        if commit_details_list:
                            labor_df = _create_summary_dataframe(commit_details_list, hours_per_commit)
                            if not labor_df.empty:
                                repo_data["laborHours"] = round(float(labor_df["EstimatedHours"].sum()), 2)
                                logger.info(f"Estimated labor hours for {repo.name}: {repo_data['laborHours']}")
                            else:
                                repo_data["laborHours"] = 0.0
                        else:
                            repo_data["laborHours"] = 0.0
                    except Exception as e_lh:
                        logger.warning(f"Could not estimate labor hours for Azure DevOps repo {repo.name}: {e_lh}", exc_info=True)
                        repo_data["laborHours"] = 0.0 # Default or None if preferred
               
                repo_data = exemption_processor.process_repository_exemptions(repo_data, default_org_identifiers=default_ids_for_exemption)
                
                processed_repo_list.append(repo_data)
                processed_counter[0] += 1

            except AzureDevOpsServiceError as ado_err_repo:
                logger.error(f"Azure DevOps API error processing repo {repo.name} in {project_name}: {ado_err_repo}. Skipping.", exc_info=False)
                processed_repo_list.append({"name": repo.name, "organization": organization_name, "_azure_project_name": project_name, "processing_error": f"Azure DevOps API Error: {ado_err_repo}"})
            except Exception as e_repo:
                logger.error(f"Unexpected error processing repo {repo.name} in {project_name}: {e_repo}. Skipping.", exc_info=True)
                processed_repo_list.append({"name": repo.name, "organization": organization_name, "_azure_project_name": project_name, "processing_error": f"Unexpected Error: {e_repo}"})
        
        logger.info(f"Fetched and initiated processing for {repo_count_for_project} repositories from Azure DevOps project: {organization_name}/{project_name}")

    except AzureDevOpsServiceError as e:
        logger.critical(f"Azure DevOps API error for {organization_name}/{project_name} (using {auth_method}): {e}", exc_info=False)
        return [] 
    except Exception as e:
        logger.critical(f"An unexpected error occurred during Azure DevOps connection or processing for {organization_name}/{project_name}: {e}", exc_info=True)
        return []

    return processed_repo_list


if __name__ == '__main__':
    # This test block now needs to simulate how generate_codejson.py would pass credentials
    # For simplicity, it might still use os.getenv for test values, but the main function above does not.
    from dotenv import load_dotenv as load_dotenv_for_test # Alias
    load_dotenv_for_test()

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Simulate getting these from CLI args for testing
    test_pat_token = os.getenv("AZURE_DEVOPS_TOKEN_TEST") 
    test_spn_client_id = os.getenv("AZURE_CLIENT_ID_TEST")
    test_spn_client_secret = os.getenv("AZURE_CLIENT_SECRET_TEST")
    test_spn_tenant_id = os.getenv("AZURE_TENANT_ID_TEST")

    raw_targets_list_env = [t.strip() for t in os.getenv("AZURE_DEVOPS_TARGETS_TEST", "").split(',') if t.strip()]
    default_org_env = os.getenv("AZURE_DEVOPS_ORG_TEST")

    test_target_full_path = None
    if raw_targets_list_env:
        first_raw_target = raw_targets_list_env[0]
        if '/' in first_raw_target:
            test_target_full_path = first_raw_target
        elif default_org_env and default_org_env != "YourAzureDevOpsOrgName":
            test_target_full_path = f"{default_org_env}/{first_raw_target}"

    auth_available = (not are_spn_details_placeholders(test_spn_client_id, test_spn_client_secret, test_spn_tenant_id) or \
                      not is_placeholder_token(test_pat_token))

    if not auth_available:
        logger.error("Neither valid SPN details (AZURE_CLIENT_ID_TEST, etc.) nor a PAT (AZURE_DEVOPS_TOKEN_TEST) found in .env for testing.")
    elif not test_target_full_path:
        logger.error("No valid Azure DevOps target found in AZURE_DEVOPS_TARGETS_TEST (with optional AZURE_DEVOPS_ORG_TEST) in .env for testing.")
    else:
        test_org_name, test_proj_name = test_target_full_path.split('/', 1)
        logger.info(f"--- Testing Azure DevOps Connector for project: {test_org_name}/{test_proj_name} ---")
        counter = [0]
        repositories = fetch_repositories(
            pat_token=test_pat_token,
            spn_client_id=test_spn_client_id,
            spn_client_secret=test_spn_client_secret,
            spn_tenant_id=test_spn_tenant_id,
            organization_name=test_org_name, 
            project_name=test_proj_name, 
            processed_counter=counter, 
            debug_limit=None
        )
        
        if repositories:
            logger.info(f"Successfully fetched {len(repositories)} repositories.")
            for i, repo_info in enumerate(repositories[:3]):
                logger.info(f"--- Repository {i+1} ({repo_info.get('name')}) ---")
                logger.info(f"  Repo ID: {repo_info.get('repo_id')}")
                # ... (rest of the print statements for repo_info) ...
                if "processing_error" in repo_info:
                    logger.error(f"  Processing Error: {repo_info['processing_error']}")
            if len(repositories) > 3:
                logger.info(f"... and {len(repositories)-3} more repositories.")
        else:
            logger.warning("No repositories fetched or an error occurred.")
        logger.info(f"Total repositories processed according to counter: {counter[0]}")
