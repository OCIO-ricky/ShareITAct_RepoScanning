# clients/azure_devops_connector.py
"""
Azure DevOps Connector for Share IT Act Repository Scanning Tool.

This module is responsible for fetching repository data from Azure DevOps,
including metadata, README content, and other relevant details.
It interacts with the Azure DevOps REST API via the azure-devops Python SDK.
"""
import os
import logging
import time
import threading # For locks
from concurrent.futures import ThreadPoolExecutor, as_completed
import base64
from typing import List, Dict, Optional, Any, Tuple
from datetime import timezone, datetime, timedelta
from utils.caching import load_previous_scan_data, PLATFORM_CACHE_CONFIG
from utils.labor_hrs_estimator import analyze_azure_devops_repo_sync # Import the labor hrs estimator
from utils.fetch_utils import (
    fetch_optional_content_with_retry,
    FETCH_ERROR_FORBIDDEN, FETCH_ERROR_NOT_FOUND, FETCH_ERROR_EMPTY_REPO_API,
    FETCH_ERROR_API_ERROR, FETCH_ERROR_UNEXPECTED
)
from utils.dateparse import get_fixed_private_filter_date # Import the consolidated utility
from utils.rate_limit_utils import get_azure_devops_rate_limit_status, calculate_inter_submission_delay # New

ANSI_YELLOW = "\x1b[33;1m"
ANSI_RESET = "\x1b[0m"
ANSI_RED = "\x1b[31;1m" # Added for consistency in warning messages


# --- Try importing Azure DevOps SDK ---
try:
    from azure.devops.connection import Connection
    from msrest.authentication import BasicAuthentication # For PAT (Personal Access Token)
    from azure.identity import ClientSecretCredential # For Service Principal with client secret
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
    GitRepository = type('GitRepository', (object,), {}) # Define dummy for type hints
    BasicAuthentication = type('BasicAuthentication', (object,), {}) # Dummy for PAT
    ClientSecretCredential = type('ClientSecretCredential', (object,), {}) # Dummy for SPN with secret
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
        #def process_repository_exemptions(self, repo_data: Dict[str, Any], default_org_identifiers: Optional[List[str]] = None) -> Dict[str, Any]:
        def process_repository_exemptions(self, repo_data: Dict[str, Any], default_org_identifiers: Optional[List[str]] = None, **kwargs: Any) -> Dict[str, Any]:
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

# --- Constants for the fetch_utils utility ---
AZURE_DEVOPS_EXCEPTION_MAP = {
    'forbidden_exception': lambda e: isinstance(e, AzureDevOpsServiceError) and hasattr(e, 'status_code') and e.status_code == 403,
    'not_found_exception': lambda e: isinstance(e, AzureDevOpsServiceError) and (
        (hasattr(e, 'status_code') and e.status_code == 404) or
        ("TF401019" in str(e)) or # "Item not found"
        ("does not exist" in str(e).lower()) # Another common not found message
    ),
    # Azure DevOps get_item_text might just return 404 if the repo is empty and file path doesn't exist.
    # A more direct empty check is repo.size == 0 or no default_branch before calling.
    'empty_repo_check_func': lambda e: False, # Placeholder, as 404 is primary for missing files
    'generic_platform_exception': AzureDevOpsServiceError
}
MAX_QUICK_CONTENT_RETRIES_AZURE = 2
QUICK_CONTENT_RETRY_DELAY_SECONDS_AZURE = 3

def is_placeholder_token(token: Optional[str]) -> bool:
    """Checks if the Azure DevOps PAT is missing or a known placeholder."""
    return not token or token == PLACEHOLDER_AZURE_TOKEN

def are_spn_details_placeholders(client_id: Optional[str], client_secret: Optional[str], tenant_id: Optional[str]) -> bool:
    """Checks if any SPN detail is missing or a known placeholder.""" # Keep this line
    return not client_id or client_id == PLACEHOLDER_AZURE_CLIENT_ID or \
           not client_secret or client_secret == PLACEHOLDER_AZURE_CLIENT_SECRET or \
           not tenant_id or tenant_id == PLACEHOLDER_AZURE_TENANT_ID

def _get_readme_content_azure_devops(
    git_client: GitClient,
    repo_id: str,
    project_name: str,
    repo_default_branch: Optional[str],
    repo_web_url: str,
    dynamic_post_api_call_delay_seconds: float,
    logger_instance: logging.LoggerAdapter, 
) -> tuple[Optional[str], Optional[str], bool]: # Added bool for is_empty_repo_error
    """Fetches README content and URL for an Azure DevOps repository."""
    # Wrapper for the dynamic delay
    def _azure_dynamic_delay_wrapper():
        if dynamic_post_api_call_delay_seconds > 0:
            logger_instance.debug(f"Applying SYNC post-API call delay (README fetch): {dynamic_post_api_call_delay_seconds:.2f}s")
            time.sleep(dynamic_post_api_call_delay_seconds)

    common_readme_names = ["README.md", "README.txt", "README"]
    if not repo_default_branch:
        logger_instance.warning(f"Cannot fetch README for repo ID {repo_id} in {project_name}: No default branch identified.")
        return None, None, False

    for readme_name in common_readme_names:
        normalized_readme_name = readme_name.lstrip('/')
        
        def fetch_lambda():
            # get_item_text returns a stream of decoded strings
            item_content_stream = git_client.get_item_text(
                repository_id=repo_id,
                path=normalized_readme_name,
                project=project_name,
                download=True, # Ensures content is fetched
                version_descriptor={'version': repo_default_branch}
            )
            return "".join(chunk for chunk in item_content_stream) # Concatenate chunks

        # The 'raw_file_object' here will be the concatenated string content
        readme_content_str, error_type = fetch_optional_content_with_retry(
            fetch_callable=fetch_lambda,
            content_description=f"README '{readme_name}'",
            repo_identifier=f"ADO:{project_name}/{repo_id}",
            platform_exception_map=AZURE_DEVOPS_EXCEPTION_MAP,
            max_quick_retries=MAX_QUICK_CONTENT_RETRIES_AZURE,
            quick_retry_delay_seconds=QUICK_CONTENT_RETRY_DELAY_SECONDS_AZURE,
            logger_instance=logger_instance, 
            dynamic_delay_func=_azure_dynamic_delay_wrapper
        )

        if error_type == FETCH_ERROR_EMPTY_REPO_API: # Should not be hit based on current map
            return None, None, True
        if error_type == FETCH_ERROR_NOT_FOUND:
            continue # Try next readme name
        if error_type in [FETCH_ERROR_FORBIDDEN, FETCH_ERROR_API_ERROR, FETCH_ERROR_UNEXPECTED]:
            logger_instance.error(f"Stopping README fetch for ADO repo ID {repo_id} due to error: {error_type}")
            return None, None, False

        if readme_content_str is not None: # Success and content is not None (empty string is valid content)
            url_readme_name = readme_name.lstrip('/')
            branch_name_for_url = repo_default_branch.replace('refs/heads/', '')
            readme_url = f"{repo_web_url}?path=/{url_readme_name}&version=GB{branch_name_for_url}&_a=contents"
            logger_instance.debug(f"Successfully fetched README '{readme_name}' for repo ID {repo_id}")
            return readme_content_str, readme_url, False

    logger_instance.debug(f"No common README file found for repo ID {repo_id}")
    return None, None, False

def _get_codeowners_content_azure_devops(
    git_client: GitClient,
    repo_id: str,
    project_name: str,
    repo_default_branch: Optional[str],
    dynamic_post_api_call_delay_seconds: float,
    logger_instance: logging.LoggerAdapter 
) -> tuple[Optional[str], bool]: # Added bool for is_empty_repo_error
    """Fetches CODEOWNERS content for an Azure DevOps repository."""
    def _azure_dynamic_delay_wrapper():
        if dynamic_post_api_call_delay_seconds > 0:
            logger_instance.debug(f"Applying SYNC post-API call delay (CODEOWNERS fetch): {dynamic_post_api_call_delay_seconds:.2f}s")
            time.sleep(dynamic_post_api_call_delay_seconds)

    codeowners_locations = ["CODEOWNERS", ".azuredevops/CODEOWNERS", "docs/CODEOWNERS", ".vsts/CODEOWNERS"]
    if not repo_default_branch:
        logger_instance.warning(f"Cannot fetch CODEOWNERS for repo ID {repo_id} in {project_name}: No default branch identified.")
        return None, False

    for location in codeowners_locations:
        normalized_location = location.lstrip('/')

        def fetch_lambda():
            item_content_stream = git_client.get_item_text(
                repository_id=repo_id,
                path=normalized_location,
                project=project_name,
                download=True,
                version_descriptor={'version': repo_default_branch}
            )
            return "".join(chunk for chunk in item_content_stream)

        codeowners_content_str, error_type = fetch_optional_content_with_retry(
            fetch_callable=fetch_lambda,
            content_description=f"CODEOWNERS from '{location}'",
            repo_identifier=f"ADO:{project_name}/{repo_id}",
            platform_exception_map=AZURE_DEVOPS_EXCEPTION_MAP,
            max_quick_retries=MAX_QUICK_CONTENT_RETRIES_AZURE,
            quick_retry_delay_seconds=QUICK_CONTENT_RETRY_DELAY_SECONDS_AZURE,
            logger_instance=logger_instance, 
            dynamic_delay_func=_azure_dynamic_delay_wrapper
        )

        if error_type == FETCH_ERROR_EMPTY_REPO_API:
            return None, True
        if error_type == FETCH_ERROR_NOT_FOUND:
            continue
        if error_type in [FETCH_ERROR_FORBIDDEN, FETCH_ERROR_API_ERROR, FETCH_ERROR_UNEXPECTED]:
            logger_instance.error(f"Stopping CODEOWNERS fetch for ADO repo ID {repo_id} due to error: {error_type}")
            return None, False

        if codeowners_content_str is not None: # Success
            logger_instance.debug(f"Successfully fetched CODEOWNERS from '{location}' for repo ID {repo_id}")
            return codeowners_content_str, False

    logger_instance.debug(f"No CODEOWNERS file found in standard locations for repo ID {repo_id}")
    return None, False

def _fetch_tags_azure_devops(
    git_client: GitClient,
    repo_id: str,
    project_name: str,
    dynamic_post_api_call_delay_seconds: float,
    logger_instance: logging.LoggerAdapter 
) -> List[str]:
    if not AZURE_SDK_AVAILABLE: return []
    tag_names = []
    try:
        logger_instance.debug(f"Fetching tags for repo ID: {repo_id} in project {project_name}")
        # Apply delay before the API call
        if dynamic_post_api_call_delay_seconds > 0:
            logger_instance.debug(f"Applying SYNC post-API call delay (tags fetch): {dynamic_post_api_call_delay_seconds:.2f}s")
            time.sleep(dynamic_post_api_call_delay_seconds)
        refs = git_client.get_refs(repo_id=repo_id, project=project_name, filter="tags/")
        for ref in refs:
            if ref.name and ref.name.startswith("refs/tags/"):
                tag_names.append(ref.name.replace("refs/tags/", ""))
        logger.debug(f"Found {len(tag_names)} tags for repo ID {repo_id}")
    except AzureDevOpsServiceError as e:
        logger_instance.error(f"Azure DevOps API error fetching tags for repo ID {repo_id}: {e}", exc_info=False)
    except Exception as e:
        logger_instance.error(f"Unexpected error fetching tags for repo ID {repo_id}: {e}", exc_info=True)
    return tag_names

def _process_single_azure_devops_repository(
    git_client: GitClient, 
    core_client: CoreClient, 
    repo: 'GitRepository', 
    organization_name: str,
    project_name: str,
    pat_token_for_estimator: Optional[str], 
    spn_client_id_for_estimator: Optional[str],
    spn_client_secret_for_estimator: Optional[str],
    spn_tenant_id_for_estimator: Optional[str],
    hours_per_commit: Optional[float],
    cfg_obj: Any, # Pass the Config object
    # --- Parameters for Caching ---
    previous_scan_cache: Dict[str, Dict],
    current_commit_sha: Optional[str],
    current_commit_date: Optional[datetime], # Date of the current_commit_sha
    dynamic_post_api_call_delay_seconds: float, # Delay for sync API calls within this function
    num_items_in_target: int, # Number of repos in the current ADO project target
    logger_instance: logging.LoggerAdapter,
    num_workers: int = 1
) -> Dict[str, Any]:
    """
    Processes a single Azure DevOps repository to extract its metadata.
    This function is intended to be run in a separate thread.
    """
    repo_full_name = f"{organization_name}/{project_name}/{repo.name}"
    repo_id_str = str(repo.id) # Key for caching
    repo_data: Dict[str, Any] = {"name": repo.name, "organization": organization_name, "_azure_project_name": project_name}
    azure_cache_config = PLATFORM_CACHE_CONFIG["azure"]
    current_logger = logger_instance # Use the passed-in logger

    # --- Caching Logic ---
    if current_commit_sha: # Only attempt cache hit if we have a current SHA to compare
        cached_repo_entry = previous_scan_cache.get(repo_id_str)
        if cached_repo_entry:
            cached_commit_sha = cached_repo_entry.get(azure_cache_config["commit_sha_field"])
            if cached_commit_sha and current_commit_sha == cached_commit_sha:
                current_logger.info(f"CACHE HIT: Azure DevOps repo '{repo_full_name}' (ID: {repo_id_str}) has not changed. Using cached data.")
                
                # Start with the cached data
                repo_data_to_process = cached_repo_entry.copy()
                # Ensure the current (and matching) SHA is in the data for consistency
                repo_data_to_process[azure_cache_config["commit_sha_field"]] = current_commit_sha
                
                 # Re-process exemptions to apply current logic/AI models, even on cached data
                default_ids_for_exemption_cache = [organization_name]
                if project_name and project_name.lower() != organization_name.lower():
                    default_ids_for_exemption_cache.append(project_name)
                if cfg_obj:
                    repo_data_to_process = exemption_processor.process_repository_exemptions(
                        repo_data_to_process,
                        scm_org_for_logging=organization_name,
                        cfg_obj=cfg_obj, 
                        default_org_identifiers=default_ids_for_exemption_cache,
                        logger_instance=current_logger)
                return repo_data_to_process # Return cached and re-processed data

    current_logger.info(f"No SHA: Processing Azure DevOps repo: {repo_full_name} (ID: {repo_id_str}) with full data fetch.")

    try:
        if repo.is_fork and repo.parent_repository:
            parent_info = "unknown parent"
            if repo.parent_repository.name and repo.parent_repository.project and repo.parent_repository.project.name:
                parent_info = f"{repo.parent_repository.project.name}/{repo.parent_repository.name}"
            elif repo.parent_repository.name:
                parent_info = repo.parent_repository.name
            current_logger.info(f"Skipping forked repository: {repo.name} (fork of {parent_info})")
            repo_data["processing_status"] = "skipped_fork"
            return repo_data

        repo_data['_is_empty_repo'] = False
        if repo.size == 0:
            current_logger.info(f"Repository {repo.name} (ID: {repo.id}) has size 0, indicating it is empty.")
            repo_data['_is_empty_repo'] = True

        created_at_iso: Optional[str] = None 
        # Repository creation date is not easily available from ADO SDK's GitRepository model.
        # If it were, it would be set here. For now, it remains None.

        pushed_at_iso: Optional[str] = None
        if current_commit_date:
            # Ensure the datetime is UTC
            pushed_at_dt = current_commit_date.astimezone(timezone.utc) if current_commit_date.tzinfo else current_commit_date.replace(tzinfo=timezone.utc)
            pushed_at_iso = pushed_at_dt.isoformat()
        elif repo.project and repo.project.last_update_time: # Fallback, less accurate
            current_logger.warning(
                f"Using project's last_update_time as fallback for repository '{repo.name}' lastModified date, "
                "as specific commit date was not available."
            )
            fallback_dt = repo.project.last_update_time.astimezone(timezone.utc) if repo.project.last_update_time.tzinfo else repo.project.last_update_time.replace(tzinfo=timezone.utc)
            pushed_at_iso = fallback_dt.isoformat()

        repo_visibility = "private" 
        try:
            project_details = core_client.get_project(project_id=project_name) 
            if project_details and project_details.visibility:
                vis = project_details.visibility.lower()
                repo_visibility = vis if vis in ["public", "private"] else "private"
            if dynamic_post_api_call_delay_seconds > 0:
                current_logger.debug(f"Azure DevOps applying SYNC post-API call delay (get project details for visibility): {dynamic_post_api_call_delay_seconds:.2f}s")
                time.sleep(dynamic_post_api_call_delay_seconds)
        except Exception as proj_vis_err:
            current_logger.warning(f"Could not determine project visibility for {repo_full_name}: {proj_vis_err}. Defaulting to 'private'.")
        
        readme_content, readme_html_url, readme_empty_err = _get_readme_content_azure_devops(
            git_client=git_client,
            repo_id=repo.id,
            project_name=project_name,
            repo_default_branch=repo.default_branch,
            repo_web_url=repo.web_url,
            dynamic_post_api_call_delay_seconds=dynamic_post_api_call_delay_seconds, # No num_workers needed here
            logger_instance=current_logger # Pass logger
        )
        codeowners_content, codeowners_empty_err = _get_codeowners_content_azure_devops(
            git_client=git_client,
            repo_id=repo.id,
            project_name=project_name,
            repo_default_branch=repo.default_branch,
            dynamic_post_api_call_delay_seconds=dynamic_post_api_call_delay_seconds, # Pass the delay
            logger_instance=current_logger # Pass logger
        )
        # The dynamic_post_api_call_delay_seconds was passed to _get_codeowners_content_azure_devops
        # and applied by its internal _azure_dynamic_delay_wrapper.
        if not repo_data.get('_is_empty_repo', False): # If not already marked empty
            repo_data['_is_empty_repo'] = readme_empty_err or codeowners_empty_err

        repo_git_tags = _fetch_tags_azure_devops( # No num_workers needed here
            git_client=git_client, repo_id=repo.id, project_name=project_name,
            dynamic_post_api_call_delay_seconds=dynamic_post_api_call_delay_seconds, logger_instance=current_logger
        )

        repo_data.update({
            "description": repo.project.description if repo.project and repo.project.description else "",
            "repositoryURL": repo.web_url, "homepageURL": repo.web_url, "downloadURL": None, "vcs": "git",
            "repositoryVisibility": repo_visibility, "status": "development", "version": "N/A", "laborHours": 0,
            "languages": [], "tags": [], 
            "date": {"created": created_at_iso, "lastModified": pushed_at_iso},
            "permissions": {"usageType": None, "exemptionText": None, "licenses": []}, # readme_content_str was a typo
            "contact": {}, "contractNumber": None, "readme_content": readme_content, # Use the fetched readme_content
            "_codeowners_content": codeowners_content, # Use the fetched codeowners_content
            "repo_id": repo.id, # Add repo_id back
            "readme_url": readme_html_url, 
            "_api_tags": repo_git_tags, "archived": repo.is_disabled if hasattr(repo, 'is_disabled') else False
        })
        repo_data.setdefault('_is_empty_repo', False)
        # Store the current commit SHA for the next scan's cache, if available
        if current_commit_sha:
            repo_data[azure_cache_config["commit_sha_field"]] = current_commit_sha


        default_ids_for_exemption = [organization_name]
        if project_name and project_name.lower() != organization_name.lower():
            default_ids_for_exemption.append(project_name)

        if hours_per_commit is not None:
            current_logger.debug(f"Estimating labor hours for Azure DevOps repo: {repo.name} in {project_name}")
            labor_df = analyze_azure_devops_repo_sync(
                organization=organization_name, project=project_name, repo_id=repo.id,
                pat_token=pat_token_for_estimator, 
                # Pass SPN details, assuming analyze_azure_devops_repo_sync can use them
                spn_client_id=spn_client_id_for_estimator,
                spn_client_secret=spn_client_secret_for_estimator,
                spn_tenant_id=spn_tenant_id_for_estimator,
                hours_per_commit=hours_per_commit,
                cfg_obj=cfg_obj, # Pass cfg_obj for its own post-API call delays
                num_repos_in_target=num_items_in_target, # Pass the count of repos in this target
                is_empty_repo=repo_data.get('_is_empty_repo', False),
                number_of_workers=num_workers, # Pass the worker count
                logger_instance=current_logger # Pass logger
            )
            repo_data["laborHours"] = round(float(labor_df["EstimatedHours"].sum()), 2) if not labor_df.empty else 0.0
            if repo_data["laborHours"] > 0: current_logger.info(f"Estimated labor hours for {repo.name}: {repo_data['laborHours']}")

        if cfg_obj:
            repo_data = exemption_processor.process_repository_exemptions(
                repo_data,
                scm_org_for_logging=organization_name,
                cfg_obj=cfg_obj, 
                default_org_identifiers=default_ids_for_exemption,
                logger_instance=current_logger)
        else:
            current_logger.warning(
                f"cfg_obj not provided to _process_single_azure_devops_repository for {repo_full_name}. "
                "Exemption processor will use its default AI parameter values."
            )
            repo_data = exemption_processor.process_repository_exemptions(
                repo_data, 
                scm_org_for_logging=organization_name,
                cfg_obj=cfg_obj, 
                default_org_identifiers=default_ids_for_exemption,
                logger_instance=current_logger
            )

        return repo_data

    except AzureDevOpsServiceError as ado_err_repo:
        current_logger.error(f"Azure DevOps API error processing repo {repo.name} in {project_name}: {ado_err_repo}. Skipping.", exc_info=False)
        return {"name": repo.name, "organization": organization_name, "_azure_project_name": project_name, "processing_error": f"Azure DevOps API Error: {ado_err_repo}"}
    except Exception as e_repo:
        current_logger.error(f"Unexpected error processing repo {repo.name} in {project_name}: {e_repo}. Skipping.", exc_info=True)
        return {"name": repo.name, "organization": organization_name, "_azure_project_name": project_name, "processing_error": f"Unexpected Error: {e_repo}"}

def _setup_azure_devops_credentials(
    pat_token: Optional[str],
    spn_client_id: Optional[str],
    spn_client_secret: Optional[str],
    spn_tenant_id: Optional[str],
    logger_instance: logging.LoggerAdapter # Expecting an adapter or base logger
) -> Tuple[Optional[Any], str]:
    """
    Sets up Azure DevOps credentials using either Service Principal or PAT.
    Returns a tuple of (credentials_object, auth_method_string).
    Returns (None, "") if authentication setup fails.
    """
    if not AZURE_SDK_AVAILABLE:
        logger_instance.error("Azure SDK not available, cannot set up credentials.")
        return None, ""

    if not are_spn_details_placeholders(spn_client_id, spn_client_secret, spn_tenant_id):
        logger_instance.info("Attempting Azure DevOps authentication using Service Principal.")
        if not ClientSecretCredential: # Check for the correctly imported class
            logger_instance.error("ClientSecretCredential class not available. Cannot use SPN auth.")
            return None, ""
        credentials = ClientSecretCredential( # Instantiate the correctly imported class
            tenant_id=spn_tenant_id, client_id=spn_client_id, client_secret=spn_client_secret # Corrected argument names
        )
        return credentials, "Service Principal"
    elif not is_placeholder_token(pat_token):
        logger_instance.info("Attempting Azure DevOps authentication using Personal Access Token (PAT).")
        if not BasicAuthentication:
            logger_instance.error("BasicAuthentication class not available. Cannot use PAT auth.")
            return None, ""
        return BasicAuthentication('', pat_token), "PAT"
    else:
        logger_instance.error("Azure DevOps authentication failed: Neither valid SPN details nor a PAT were provided, or they are placeholders.")
        return None, ""

def _get_repo_stubs_and_estimate_api_calls(
    git_client: GitClient,
    organization_name: str, # For logging
    project_name: str, # For API calls and logging
    fixed_private_filter_date: datetime,
    hours_per_commit: Optional[float],
    cfg_obj: Any,
    logger_instance: logging.Logger,
    previous_scan_cache: Dict[str, Dict] # NEW: Pass the cache
) -> tuple[List[GitRepository], int]:
    """
    Internal helper to list repository stubs, filter them, and estimate API calls.
    Returns a list of repository stubs to process and the estimated API calls for them.
    """
    logger_instance.info(f"{ANSI_YELLOW}Pre-scanning{ANSI_RESET} all repository stubs for project '{project_name}' in org '{organization_name}'... Be patient!")

    azure_cache_config = PLATFORM_CACHE_CONFIG["azure"]

    all_repo_stubs_in_project = []
    try:
        all_repo_stubs_in_project = list(git_client.get_repositories(project=project_name))
        logger_instance.info(f"Found {len(all_repo_stubs_in_project)} total repository stubs for '{project_name}'.")
    except AzureDevOpsServiceError as rle_list:
        logger_instance.error(f"Azure DevOps API error while listing repositories for '{project_name}': {rle_list}. Cannot proceed.")
        raise
    except Exception as e_list:
        logger_instance.error(f"Error listing repositories for '{project_name}': {e_list}. Cannot proceed.", exc_info=True)
        raise

    repos_to_process_stubs = []
    estimated_api_calls_for_target = 0
    # Estimate listing calls: 1 call per page (typically many repos per page for ADO, but let's be simple)
    # This is a rough estimate; ADO's get_repositories is efficient.
    estimated_api_calls_for_target += 1 # At least one call to list.
    skipped_empty_repo_count = 0 # New counter
    skipped_by_date_filter_count = 0

    for repo_stub in all_repo_stubs_in_project:
        include_repo = False
        # Visibility in ADO is often at the project level. Repo stubs might not have individual visibility.
        # We assume the project's visibility (fetched later or passed) applies.
        # For estimation, we might need to assume public or make a call to get project visibility if not available.
        # Here, we'll rely on the project's visibility determined in the main fetch_repositories.
        # For now, let's assume if it's not explicitly private by date, it's included for estimation.
        # This part might need refinement if project visibility is crucial for estimation and not readily available.
        
        # Simplified filter for estimation: if it's a private repo (based on project visibility, which we don't have here directly)
        # then apply date filter. For now, assume all are potentially processable for estimation count.
        # A more accurate estimation would require fetching project visibility first.
        # For this estimation function, we'll assume the filter in fetch_repositories is the source of truth.
        # This function will just count based on what `fetch_repositories` would filter.
        
        # Replicating the filter logic from fetch_repositories for consistency in estimation:
        project_visibility = repo_stub.project.visibility.lower() if repo_stub.project and repo_stub.project.visibility else "private"
        if project_visibility == "public":
            include_repo = True
        else: # Private or internal project repo, apply date filter
            modified_at_dt = repo_stub.project.last_update_time # This is project's last update time
            if modified_at_dt and modified_at_dt.tzinfo is None: modified_at_dt = modified_at_dt.replace(tzinfo=timezone.utc)
            if modified_at_dt and modified_at_dt >= fixed_private_filter_date:
                include_repo = True
            else:
                skipped_by_date_filter_count += 1
        
        # NEW: Add check for empty repository using repo_stub.size
        if include_repo: # If it's still a candidate after privacy/date filters
            # Check if the repository is empty using the 'size' attribute from the REST API stub.
            if hasattr(repo_stub, 'size') and repo_stub.size == 0:
                logger_instance.info(f"Pre-scan: ADO repo '{repo_stub.name}' in project '{project_name}' identified as empty (size: 0 from REST stub). Skipping further processing for this repo in estimation phase.")
                include_repo = False
                skipped_empty_repo_count += 1
        
        if include_repo:
            repos_to_process_stubs.append(repo_stub)
            made_sha_call_in_estimation = False # Flag to track if SHA call was made during this estimation step

            # --- Cache Check for Estimation ---
            is_likely_cached = False
            repo_id_str = str(repo_stub.id)
            
            if repo_id_str in previous_scan_cache:
                cached_entry = previous_scan_cache[repo_id_str]
                # Heuristic for estimation: if repo_id is in cache, assume fewer calls.
                # Avoid live SHA fetch here to keep estimation light.
                # The main fetch_repositories loop will do the accurate SHA check.
                cached_last_modified_str = cached_entry.get('date', {}).get('lastModified')
                # Project's last_update_time is a rough proxy for repo's pushed_at for ADO stubs
                stub_last_activity_dt = repo_stub.project.last_update_time if repo_stub.project else None
                if stub_last_activity_dt and cached_last_modified_str:
                    try:
                        if stub_last_activity_dt.tzinfo is None: stub_last_activity_dt = stub_last_activity_dt.replace(tzinfo=timezone.utc)
                        cached_dt = datetime.fromisoformat(cached_last_modified_str.replace('Z', '+00:00')).replace(tzinfo=timezone.utc)
                        if stub_last_activity_dt <= cached_dt: # If project hasn't updated since cache
                            is_likely_cached = True
                            logger_instance.debug(f"Pre-scan: ADO repo {repo_stub.name} (ID: {repo_id_str}) likely cached based on project last_update_time. Skipping detailed call estimates.")
                    except ValueError:
                        logger_instance.warning(f"Pre-scan: Could not parse date for ADO repo {repo_stub.name} for cache check during estimation.")

            if not is_likely_cached:
                # If not cached, and SHA call wasn't made during cache check (e.g. repo not in previous_scan_cache)
                # account for the SHA call that fetch_repositories will make.
                if not made_sha_call_in_estimation:
                    estimated_api_calls_for_target += 1 # For the SHA call in fetch_repositories
                estimated_api_calls_for_target += 5 # Adjusted metadata: project details, README, CODEOWNERS, tags, buffer
                if hours_per_commit is not None and hours_per_commit > 0:
                    estimated_api_calls_for_target += getattr(cfg_obj, 'ESTIMATED_LABOR_CALLS_PER_REPO_AZURE_ENV',
                                                              getattr(cfg_obj, 'ESTIMATED_LABOR_CALLS_PER_REPO_ENV', 3))
            # --- End Cache Check ---


    logger_instance.info(f"Identified {len(repos_to_process_stubs)} repositories to estimate for in detail for '{project_name}'.")
    if skipped_by_date_filter_count > 0:
        logger_instance.info(f"Skipped {skipped_by_date_filter_count} private project repositories from '{project_name}' due to fixed date filter ({fixed_private_filter_date.strftime('%Y-%m-%d')}) for estimation.")
    if skipped_empty_repo_count > 0:
        logger_instance.info(f"Skipped {skipped_empty_repo_count} empty repositories from '{project_name}' during pre-scan estimation.")
    return repos_to_process_stubs, estimated_api_calls_for_target

def fetch_repositories(
    token: Optional[str],
    target_path: str,
    processed_counter: List[int],
    processed_counter_lock: threading.Lock,
    logger_instance: logging.LoggerAdapter, # Made non-optional
    debug_limit: int | None = None,
    azure_devops_url: str | None = None,
    hours_per_commit: Optional[float] = None,
    max_workers: int = 5,  # Ensure this parameter exists
    cfg_obj: Optional[Any] = None,
    previous_scan_output_file: Optional[str] = None,
    spn_client_id: Optional[str] = None, # Renamed for clarity to match internal usage
    spn_client_secret: Optional[str] = None, # Renamed
    spn_tenant_id: Optional[str] = None
 ) -> list[dict]:
    """
    Fetches repository details from a specific Azure DevOps organization/project.
    
    Args:
        token: The Azure DevOps Personal Access Token.
        target_path: The full path in format 'organization/project'.
        processed_counter: Mutable list to track processed repositories for debug limit.
        processed_counter_lock: Lock for safely updating processed_counter.
        debug_limit: Optional global limit for repositories to process.
        azure_devops_url: The base URL of the Azure DevOps instance. Defaults to https://dev.azure.com if None.
        hours_per_commit: Optional factor to estimate labor hours based on commit count.
        max_workers: Number of concurrent worker threads for repository processing.
                     This affects rate limiting calculations.
        cfg_obj: Configuration object containing settings for API calls, delays, and exemption processing.
        previous_scan_output_file: Path to previous scan results for caching optimization.
        spn_client_id: Service Principal Client ID (alternative to token).
        spn_client_secret: Service Principal Client Secret (alternative to token).
        spn_tenant_id: Service Principal Tenant ID (alternative to token).
    
    Returns:
        A list of dictionaries, each containing processed metadata for a repository.
    """
    if not AZURE_SDK_AVAILABLE:
        logger.error("Azure DevOps SDK not available. Skipping Azure DevOps scan.")
        return []
    
    if '/' not in target_path:
        # Use module logger if target_specific_logger isn't created yet
        logging.getLogger(__name__).error(f"Invalid Azure DevOps target_path format: '{target_path}'. Expected 'organization/project'.")
        return []
    organization_name, project_name = target_path.split('/', 1)

    # Create a LoggerAdapter with the target context
    current_logger = logger_instance # Directly use the passed-in adapter
    current_logger.info(f"Attempting to fetch repositories for ADO organization: {ANSI_YELLOW}{current_logger.extra['org_group']}{ANSI_RESET} (max_workers: {max_workers})")
    fixed_private_filter_date = get_fixed_private_filter_date(cfg_obj, current_logger)

    # --- Load Previous Scan Data for Caching ---
    previous_scan_cache: Dict[str, Dict] = {}
    if previous_scan_output_file:
        current_logger.info(f"Attempting to load previous Azure DevOps scan data for '{organization_name}/{project_name}' from: {previous_scan_output_file}")
        previous_scan_cache = load_previous_scan_data(previous_scan_output_file, "azure")
    else:
        current_logger.info(f"No previous scan output file provided. Full scan for all repos in this target.")

    effective_ado_url = azure_devops_url if azure_devops_url else os.getenv("AZURE_DEVOPS_API_URL", "https://dev.azure.com")
    organization_url = f"{effective_ado_url.strip('/')}/{organization_name}" # This is correct

    processed_repo_list: List[Dict[str, Any]] = []

    try:
        credentials, auth_method = _setup_azure_devops_credentials(
            pat_token=token, # Assuming 'token' here is the PAT if SPN is not used
            spn_client_id=spn_client_id,
            spn_client_secret=spn_client_secret,
            spn_tenant_id=spn_tenant_id,
            logger_instance=current_logger 
        )
        if not credentials:
            return []

        connection = Connection(base_url=organization_url, creds=credentials)
        # --- SSL Verification Control ---
        disable_ssl_env = os.getenv("DISABLE_SSL_VERIFICATION", "false").lower()
        if disable_ssl_env == "true":
            connection.session.verify = False # Disable SSL verification for the requests session
            current_logger.warning(f"{ANSI_RED}SECURITY WARNING: SSL certificate verification is DISABLED for Azure DevOps connections due to DISABLE_SSL_VERIFICATION=true.{ANSI_RESET}")
            current_logger.warning(f"{ANSI_YELLOW}This should ONLY be used for trusted internal environments. Do NOT use in production with public-facing services.{ANSI_RESET}")
        # --- End SSL Verification Control ---

        git_client: GitClient = connection.clients.get_git_client()
        core_client: CoreClient = connection.clients.get_core_client()

        current_logger.info(f"Successfully established connection to Azure DevOps organization: {organization_name} using {auth_method}.")
        try:
            repos_to_process_stubs, estimated_api_calls_for_current_target = _get_repo_stubs_and_estimate_api_calls(
                git_client, organization_name, project_name, fixed_private_filter_date,
                hours_per_commit, cfg_obj, current_logger, previous_scan_cache # Pass current_logger and cache
            )
        except Exception: # Errors from listing/estimation
            return []

        if not repos_to_process_stubs:
            current_logger.info(f"No repositories to process for '{project_name}' after filtering. Skipping.")
            return []

        current_rate_limit_status = get_azure_devops_rate_limit_status(connection, organization_name, current_logger)
        if not current_rate_limit_status:
            current_logger.error(f"Could not determine current rate limit for '{project_name}' after listing. Aborting target.")
            return []

        platform_total_estimated_api_calls = getattr(cfg_obj, 'AZURE_TOTAL_ESTIMATED_API_CALLS', None)
        effective_estimated_calls_for_delay_calc = platform_total_estimated_api_calls \
            if platform_total_estimated_api_calls is not None and platform_total_estimated_api_calls > 0 \
            else estimated_api_calls_for_current_target
        # (Logging for which estimate is used can be added here if desired)

        # --- Calculate dynamic POST-API-CALL delay for metadata calls within this target ---
        # This delay is applied for synchronous API calls made by each worker thread.
        dynamic_post_api_call_delay_seconds = 0.0
        if cfg_obj:
            base_delay = float(getattr(cfg_obj, 'AZURE_DEVOPS_POST_API_CALL_DELAY_SECONDS_ENV', os.getenv("AZURE_DEVOPS_POST_API_CALL_DELAY_SECONDS", "0.0")))
            threshold = int(getattr(cfg_obj, 'DYNAMIC_DELAY_THRESHOLD_REPOS_ENV', os.getenv("DYNAMIC_DELAY_THRESHOLD_REPOS", "100")))
            # Scale factor for dynamic delay calculation
            scale = float(getattr(cfg_obj, 'DYNAMIC_DELAY_SCALE_FACTOR_ENV', os.getenv("DYNAMIC_DELAY_SCALE_FACTOR", "1.5")))
            max_d = float(getattr(cfg_obj, 'DYNAMIC_DELAY_MAX_SECONDS_ENV', os.getenv("DYNAMIC_DELAY_MAX_SECONDS", "1.0")))
            
            from utils.delay_calculator import calculate_dynamic_delay # Local import if not at top
            dynamic_post_api_call_delay_seconds = calculate_dynamic_delay(
                base_delay_seconds=base_delay,
                num_items=len(repos_to_process_stubs), # Use count of repos to process
                threshold_items=threshold, 
                scale_factor=scale, 
                max_delay_seconds=max_d,
                num_workers=max_workers
            )
            if dynamic_post_api_call_delay_seconds > 0:
                 current_logger.info(f"{ANSI_YELLOW}DYNAMIC POST-API-CALL delay for ADO metadata set to: {dynamic_post_api_call_delay_seconds:.2f}s (based on {len(repos_to_process_stubs)} repositories, {max_workers} workers, scale: {scale}).{ANSI_RESET}")

        # --- Calculate inter-submission delay ---
        inter_submission_delay = calculate_inter_submission_delay(
            rate_limit_status=current_rate_limit_status,
            estimated_api_calls_for_target=effective_estimated_calls_for_delay_calc,
            num_workers=max_workers,
            safety_factor=getattr(cfg_obj, 'API_SAFETY_FACTOR_ENV', 0.8),
            min_delay_seconds=getattr(cfg_obj, 'MIN_INTER_REPO_DELAY_SECONDS_ENV', 0.1),
            max_delay_seconds=getattr(cfg_obj, 'MAX_INTER_REPO_DELAY_SECONDS_ENV', 30.0)
        )

        azure_cache_config = PLATFORM_CACHE_CONFIG["azure"] # For peek logic
        num_repos_for_target_delay_calc = len(repos_to_process_stubs)

        repo_count_for_project_submitted = 0

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_repo_name = {}
            try:
                for repo_stub in repos_to_process_stubs: # Iterate the filtered list
                    with processed_counter_lock:
                        if debug_limit is not None and processed_counter[0] >= debug_limit:
                            current_logger.info(f"Global debug limit ({debug_limit}) reached. Stopping further repository submissions for {organization_name}/{project_name}.")
                            break
                        processed_counter[0] += 1
                    
                    repo_stub_full_name_for_log = f"{organization_name}/{project_name}/{repo_stub.name}"

                    # --- Get current commit SHA for caching comparison ---
                    current_commit_sha_for_cache = None
                    current_commit_date_for_cache = None
                    try:
                        if repo_stub.size == 0:
                             current_logger.info(f"Repo {repo_stub_full_name_for_log} has size 0. Cannot get current commit SHA for caching.")
                        elif repo_stub.default_branch:
                            # Apply delay before this API call
                            if dynamic_post_api_call_delay_seconds > 0:
                                current_logger.debug(f"Applying SYNC post-API call delay (get_commits for SHA): {dynamic_post_api_call_delay_seconds:.2f}s")
                                time.sleep(dynamic_post_api_call_delay_seconds)

                            search_criteria = {'itemVersion.version': repo_stub.default_branch, '$top': 1}
                            commits = git_client.get_commits(repository_id=repo_stub.id, project=project_name, search_criteria=search_criteria, top=1)
                            if commits:
                                current_commit_sha_for_cache = commits[0].commit_id
                                current_commit_date_for_cache = commits[0].committer.date # datetime object
                                current_logger.debug(f"Successfully fetched current commit SHA '{current_commit_sha_for_cache}' and date '{current_commit_date_for_cache}' for default branch '{repo_stub.default_branch}' of {repo_stub_full_name_for_log}.")
                    except AzureDevOpsServiceError as e_sha_fetch:
                        current_logger.warning(f"API error fetching current commit SHA for {repo_stub_full_name_for_log}: {e_sha_fetch}. Proceeding without SHA for caching.")
                    except Exception as e_sha_unexpected:
                        current_logger.error(f"Unexpected error fetching current commit SHA for {repo_stub_full_name_for_log}: {e_sha_unexpected}. Proceeding without SHA for caching.", exc_info=True)

                    # --- Peek-Ahead Delay Logic for Azure DevOps ---
                    actual_delay_this_submission = inter_submission_delay
                    log_message_suffix = f"Using standard submission delay: {actual_delay_this_submission:.3f}s"
                    repo_id_for_peek_key = str(repo_stub.id) # ADO repo ID is a string (GUID)

                    if cfg_obj and inter_submission_delay > cfg_obj.PEEK_AHEAD_THRESHOLD_DELAY_SECONDS_ENV:
                        if repo_stub.size == 0:
                            log_message_suffix = f"Peek: Repo is empty. {log_message_suffix}" # Standard delay will apply
                            current_logger.info(f"Peek-ahead for {repo_stub_full_name_for_log} indicates it's empty. {log_message_suffix}", extra={'org_group': f"{organization_name}/{project_name}"})
                        elif current_commit_sha_for_cache and repo_id_for_peek_key in previous_scan_cache:
                            cached_repo_entry = previous_scan_cache[repo_id_for_peek_key]
                            cached_commit_sha = cached_repo_entry.get(azure_cache_config["commit_sha_field"])
                            if cached_commit_sha == current_commit_sha_for_cache:
                                actual_delay_this_submission = cfg_obj.CACHE_HIT_SUBMISSION_DELAY_SECONDS_ENV # This line was likely the source of the error if mis-indented
                                log_message_suffix = f"Peek: Cache HIT. Using shorter submission delay: {actual_delay_this_submission:.3f}s"
                            else:
                                log_message_suffix = (f"Peek: Cache MISS (SHA changed: cached='{str(cached_commit_sha)[:7]}...', "
                                                      f"current='{str(current_commit_sha_for_cache)[:7]}...'). Using standard delay: {inter_submission_delay:.3f}s")
                        elif current_commit_sha_for_cache: # Has SHA but not in cache
                            log_message_suffix = f"Peek: Not in cache (or no previous SHA). Using standard delay: {inter_submission_delay:.3f}s"
                        # If current_commit_sha_for_cache is None (fetch failed), standard delay applies.
                    # The following lines should be at the same indentation level as the `if cfg_obj...` line above,
                    # or correctly indented if they are meant to be part of an outer block.
                    # Assuming they are part of the main loop for each enriched_repo:
                    current_logger.info(f"Delay for {repo_stub_full_name_for_log}: {log_message_suffix}", extra={'org_group': f"{organization_name}/{project_name}"})
                    if actual_delay_this_submission > 0:
                        time.sleep(actual_delay_this_submission)
                    repo_count_for_project_submitted += 1
                    future = executor.submit(
                        _process_single_azure_devops_repository,
                        git_client,
                        core_client,
                        repo_stub,
                        organization_name,
                        project_name,
                        token, 
                        spn_client_id, 
                        spn_client_secret,
                        spn_tenant_id,
                        hours_per_commit,
                        cfg_obj,
                        previous_scan_cache=previous_scan_cache,
                        current_commit_sha=current_commit_sha_for_cache,
                        current_commit_date=current_commit_date_for_cache,
                        num_items_in_target=num_repos_for_target_delay_calc, # Pass the count
                        dynamic_post_api_call_delay_seconds=dynamic_post_api_call_delay_seconds,
                        num_workers=max_workers,
                        logger_instance=current_logger # Pass the logger
                    )
                    future_to_repo_name[future] = f"{organization_name}/{project_name}/{repo_stub.name}"
            
            except AzureDevOpsServiceError as ado_list_err: # Should be caught earlier now
                current_logger.error(f"API error during repository iteration. Processing submitted tasks. Details: {ado_list_err}")
            except Exception as ex_iter:
                current_logger.error(f"Unexpected error during repository iteration: {ex_iter}. Processing submitted tasks.")

            for future in as_completed(future_to_repo_name):
                repo_name_for_log = future_to_repo_name[future]
                try:
                    repo_data_result = future.result()
                    if repo_data_result:
                        if repo_data_result.get("processing_status") == "skipped_fork":
                            pass 
                        else:
                            processed_repo_list.append(repo_data_result)
                except Exception as exc:
                    current_logger.error(f"Repository {repo_name_for_log} generated an exception in its thread: {exc}", exc_info=True)
                    name_parts = repo_name_for_log.split('/')
                    repo_n = name_parts[-1] if len(name_parts) > 0 else "UnknownRepo"
                    org_n = name_parts[0] if len(name_parts) > 1 else organization_name
                    proj_n = name_parts[1] if len(name_parts) > 2 else project_name

                    processed_repo_list.append({"name": repo_n, 
                                                "organization": org_n, 
                                                "_azure_project_name": proj_n,
                                                "processing_error": f"Thread execution failed: {exc}"})

        current_logger.info(f"Finished processing for {repo_count_for_project_submitted} repositories. Collected {len(processed_repo_list)} results.")

    except AzureDevOpsServiceError as e:
        (current_logger if 'current_logger' in locals() else logging.getLogger(__name__)).critical(
            f"Azure DevOps API error (using {auth_method}): {e}", exc_info=False)
        return [] 
    except Exception as e:
        (current_logger if 'current_logger' in locals() else logging.getLogger(__name__)).critical(
            f"An unexpected error occurred during Azure DevOps connection or processing: {e}", exc_info=True)
        return []

    return processed_repo_list

def estimate_api_calls_for_target(
    pat_token: Optional[str], # PAT token
    target_path: str, # "org/project"
    azure_devops_url: Optional[str],
    cfg_obj: Any,
    logger_instance: logging.LoggerAdapter, # Made non-optional
    spn_client_id: Optional[str] = None,
    spn_client_secret: Optional[str] = None,
    spn_tenant_id: Optional[str] = None
) -> int:
    """Estimates API calls for a given Azure DevOps org/project target."""
    if not AZURE_SDK_AVAILABLE or '/' not in target_path: return 0
    organization_name, project_name = target_path.split('/', 1)
    current_logger = logger_instance # Directly use the passed-in adapter
    current_logger.info(f"Estimating API calls for Azure DevOps target: {target_path}")

    credentials, _ = _setup_azure_devops_credentials(pat_token, spn_client_id, spn_client_secret, spn_tenant_id, current_logger)
    if not credentials: return 0

    effective_ado_url = azure_devops_url if azure_devops_url else os.getenv("AZURE_DEVOPS_API_URL", "https://dev.azure.com")
    organization_url = f"{effective_ado_url.strip('/')}/{organization_name}"
    connection = Connection(base_url=organization_url, creds=credentials)
    git_client: GitClient = connection.clients.get_git_client()
    fixed_date = get_fixed_private_filter_date(cfg_obj, current_logger)

    # Load cache for estimation - estimate_api_calls_for_target needs it
    previous_intermediate_filepath = os.path.join(getattr(cfg_obj, 'OUTPUT_DIR', '.'), f"intermediate_azure_{organization_name.replace('/', '_')}_{project_name.replace('/', '_')}.json")
    previous_scan_cache_for_estimation = load_previous_scan_data(previous_intermediate_filepath, "azure")
    
    hpc_val = None
    if hasattr(cfg_obj, 'HOURS_PER_COMMIT_ENV') and cfg_obj.HOURS_PER_COMMIT_ENV is not None:
        hpc_val = float(cfg_obj.HOURS_PER_COMMIT_ENV)

    _, estimated_calls = _get_repo_stubs_and_estimate_api_calls(git_client, organization_name, project_name, fixed_date, hpc_val, cfg_obj, current_logger, previous_scan_cache_for_estimation)
    return estimated_calls
