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
from datetime import timezone, datetime
from utils.delay_calculator import calculate_dynamic_delay # Import the calculator
from utils.dateparse import parse_repos_created_after_date # Import the new utility
from utils.caching import load_previous_scan_data, PLATFORM_CACHE_CONFIG
from utils.labor_hrs_estimator import _create_summary_dataframe # Import the labor hrs estimator

ANSI_YELLOW = "\x1b[33;1m"
ANSI_RESET = "\x1b[0m"
ANSI_RED = "\x1b[31;1m" # Added for consistency in warning messages


# --- Try importing Azure DevOps SDK ---
try:
    from azure.devops.connection import Connection
    from msrest.authentication import BasicAuthentication, ServicePrincipalCredentials
    from azure.devops.v7_1.git import GitClient
    from azure.devops.v7_1.core import CoreClient
    from azure.devops.v7_1.git.models import GitRepository # type: ignore
    from azure.devops.exceptions import AzureDevOpsServiceError
    AZURE_SDK_AVAILABLE = True
except ImportError as e:
    AZURE_SDK_AVAILABLE = False
    GitClient = type('GitClient', (object,), {})
    CoreClient = type('CoreClient', (object,), {})
    AzureDevOpsServiceError = type('AzureDevOpsServiceError', (Exception,), {})
    Connection = type('Connection', (object,), {})
    GitRepository = type('GitRepository', (object,), {}) # Define dummy for type hints
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
    """Checks if any SPN detail is missing or a known placeholder.""" # Keep this line
    return not client_id or client_id == PLACEHOLDER_AZURE_CLIENT_ID or \
           not client_secret or client_secret == PLACEHOLDER_AZURE_CLIENT_SECRET or \
           not tenant_id or tenant_id == PLACEHOLDER_AZURE_TENANT_ID


def _get_file_content_azure(git_client: GitClient, repository_id: str, project_name: str, file_path: str, repo_default_branch: Optional[str]) -> Optional[str]:
    if not AZURE_SDK_AVAILABLE: return None
    if not repo_default_branch: # Keep this line
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


def _get_readme_content_azure_devops(
    git_client: GitClient,
    repo_id: str,
    project_name: str,
    repo_default_branch: Optional[str],
    repo_web_url: str,
    cfg_obj: Optional[Any],
    dynamic_delay_to_apply: float,
    num_workers: int = 1
) -> tuple[Optional[str], Optional[str]]:
    common_readme_names = ["README.md", "README.txt", "README"]
    if not repo_default_branch:
        logger.warning(f"Cannot fetch README for repo ID {repo_id} in {project_name}: No default branch identified.")
        return None, None
    for readme_name in common_readme_names:
        content = _get_file_content_azure(git_client, repository_id, project_name, readme_name, repo_default_branch)
        if content:
            url_readme_name = readme_name.lstrip('/')
            branch_name_for_url = repo_default_branch.replace('refs/heads/', '')
            readme_url = f"{repo_web_url}?path=/{url_readme_name}&version=GB{branch_name_for_url}&_a=contents"
            logger.debug(f"Successfully fetched README '{readme_name}' for repo ID {repo_id}")
            if dynamic_delay_to_apply > 0:
                logger.debug(f"Azure DevOps applying SYNC post-API call delay (get README file): {dynamic_delay_to_apply:.2f}s")
                time.sleep(dynamic_delay_to_apply)
            return content, readme_url
    logger.debug(f"No common README file found for repo ID {repo_id}")
    return None, None

def _get_codeowners_content_azure_devops(
    git_client: GitClient,
    repo_id: str,
    project_name: str,
    repo_default_branch: Optional[str],
    cfg_obj: Optional[Any],
    dynamic_delay_to_apply: float,
    num_workers: int = 1
) -> Optional[str]:
    codeowners_locations = ["CODEOWNERS", ".azuredevops/CODEOWNERS", "docs/CODEOWNERS", ".vsts/CODEOWNERS"]
    if not repo_default_branch:
        logger.warning(f"Cannot fetch CODEOWNERS for repo ID {repo_id} in {project_name}: No default branch identified.")
        return None
    for location in codeowners_locations:
        normalized_location = location.lstrip('/')
        content = _get_file_content_azure(git_client, repository_id, project_name, normalized_location, repo_default_branch)
        if content:
            logger.debug(f"Successfully fetched CODEOWNERS from '{location}' for repo ID {repo_id}")
            if dynamic_delay_to_apply > 0:
                logger.debug(f"Azure DevOps applying SYNC post-API call delay (get CODEOWNERS file): {dynamic_delay_to_apply:.2f}s")
                time.sleep(dynamic_delay_to_apply)
            return content
    logger.debug(f"No CODEOWNERS file found in standard locations for repo ID {repo_id}")
    return None

def _fetch_tags_azure_devops(
    git_client: GitClient,
    repo_id: str,
    project_name: str,
    cfg_obj: Optional[Any],
    dynamic_delay_to_apply: float,
    num_workers: int = 1
) -> List[str]:
    if not AZURE_SDK_AVAILABLE: return []
    tag_names = []
    try:
        logger.debug(f"Fetching tags for repo ID: {repo_id} in project {project_name}")
        refs = git_client.get_refs(repository_id=repository_id, project=project_name, filter="tags/")
        if dynamic_delay_to_apply > 0:
            logger.debug(f"Azure DevOps applying SYNC post-API call delay (get tags/refs): {dynamic_delay_to_apply:.2f}s")
            time.sleep(dynamic_delay_to_apply)
        for ref in refs:
            if ref.name and ref.name.startswith("refs/tags/"):
                tag_names.append(ref.name.replace("refs/tags/", ""))
        logger.debug(f"Found {len(tag_names)} tags for repo ID {repo_id}")
    except AzureDevOpsServiceError as e:
        logger.error(f"Azure DevOps API error fetching tags for repo ID {repo_id}: {e}", exc_info=False)
    except Exception as e:
        logger.error(f"Unexpected error fetching tags for repo ID {repo_id}: {e}", exc_info=True)
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
    inter_repo_adaptive_delay_seconds: float, # Inter-repository adaptive delay
    dynamic_post_api_call_delay_seconds: float, # Per-API call dynamic delay
    # --- Parameters for Caching ---
    previous_scan_cache: Dict[str, Dict],
    current_commit_sha: Optional[str],
    num_workers: int = 1  # Add this parameter
) -> Dict[str, Any]:
    """
    Processes a single Azure DevOps repository to extract its metadata.
    This function is intended to be run in a separate thread.
    """
    repo_full_name = f"{organization_name}/{project_name}/{repo.name}"
    repo_id_str = str(repo.id) # Key for caching
    repo_data: Dict[str, Any] = {"name": repo.name, "organization": organization_name, "_azure_project_name": project_name}
    azure_cache_config = PLATFORM_CACHE_CONFIG["azure"]

    # --- Caching Logic ---
    if current_commit_sha: # Only attempt cache hit if we have a current SHA to compare
        cached_repo_entry = previous_scan_cache.get(repo_id_str)
        if cached_repo_entry:
            cached_commit_sha = cached_repo_entry.get(azure_cache_config["commit_sha_field"])
            if cached_commit_sha and current_commit_sha == cached_commit_sha:
                logger.info(f"CACHE HIT: Azure DevOps repo '{repo_full_name}' (ID: {repo_id_str}) has not changed. Using cached data.")
                
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
                        repo_data_to_process, default_org_identifiers=default_ids_for_exemption_cache,
                        ai_is_enabled_from_config=cfg_obj.AI_ENABLED_ENV, ai_model_name_from_config=cfg_obj.AI_MODEL_NAME_ENV,
                        ai_temperature_from_config=cfg_obj.AI_TEMPERATURE_ENV, ai_max_output_tokens_from_config=cfg_obj.AI_MAX_OUTPUT_TOKENS_ENV,
                        ai_max_input_tokens_from_config=cfg_obj.MAX_TOKENS_ENV)
                return repo_data_to_process # Return cached and re-processed data

    logger.info(f"No SHA: Processing Azure DevOps repo: {repo_full_name} (ID: {repo_id_str}) with full data fetch.")

    try:
        if repo.is_fork and repo.parent_repository:
            parent_info = "unknown parent"
            if repo.parent_repository.name and repo.parent_repository.project and repo.parent_repository.project.name:
                parent_info = f"{repo.parent_repository.project.name}/{repo.parent_repository.name}"
            elif repo.parent_repository.name:
                parent_info = repo.parent_repository.name
            logger.info(f"Skipping forked repository: {repo.name} (fork of {parent_info})")
            repo_data["processing_status"] = "skipped_fork"
            return repo_data

        repo_data['_is_empty_repo'] = False
        if repo.size == 0:
            logger.info(f"Repository {repo.name} (ID: {repo.id}) has size 0, indicating it is empty.")
            repo_data['_is_empty_repo'] = True

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
                repo_visibility = vis if vis in ["public", "private"] else "private"
            if dynamic_post_api_call_delay_seconds > 0:
                logger.debug(f"Azure DevOps applying SYNC post-API call delay (get project details for visibility): {dynamic_post_api_call_delay_seconds:.2f}s")
                time.sleep(dynamic_post_api_call_delay_seconds)
        except Exception as proj_vis_err:
            logger.warning(f"Could not determine project visibility for {repo_full_name}: {proj_vis_err}. Defaulting to 'private'.")
        
        readme_content, readme_html_url = _get_readme_content_azure_devops(
            git_client=git_client,
            repo_id=repo.id,
            project_name=project_name,
            repo_default_branch=repo.default_branch,
            repo_web_url=repo.web_url,
            cfg_obj=cfg_obj,
            dynamic_delay_to_apply=dynamic_post_api_call_delay_seconds,
            num_workers=num_workers
        )
        codeowners_content = _get_codeowners_content_azure_devops(
            git_client=git_client, repo_id=repo.id, project_name=project_name, repo_default_branch=repo.default_branch,
            cfg_obj=cfg_obj, dynamic_delay_to_apply=dynamic_post_api_call_delay_seconds, num_workers=num_workers
        )
        repo_git_tags = _fetch_tags_azure_devops(
            git_client=git_client, repo_id=repo.id, project_name=project_name,
            cfg_obj=cfg_obj, dynamic_delay_to_apply=dynamic_post_api_call_delay_seconds, num_workers=num_workers
        )

        repo_data.update({
            "description": repo.project.description if repo.project and repo.project.description else "",
            "repositoryURL": repo.web_url, "homepageURL": repo.web_url, "downloadURL": None, "vcs": "git",
            "repositoryVisibility": repo_visibility, "status": "development", "version": "N/A", "laborHours": 0,
            "languages": [], "tags": [], 
            "date": {"created": created_at_iso, "lastModified": pushed_at_iso},
            "permissions": {"usageType": "openSource", "exemptionText": None, "licenses": []}, # readme_content_str was a typo
            "contact": {}, "contractNumber": None, "readme_content": readme_content, # Use the fetched readme_content
            "_codeowners_content": codeowners_content_str,
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
            logger.debug(f"Estimating labor hours for Azure DevOps repo: {repo.name} in {project_name}")
            from utils.labor_hrs_estimator import analyze_azure_devops_repo_sync 
            labor_df = analyze_azure_devops_repo_sync(
                organization=organization_name, project=project_name, repo_id=repo.id,
                pat_token=pat_token_for_estimator, 
                hours_per_commit=hours_per_commit,
                cfg_obj=cfg_obj, # Pass cfg_obj for its own post-API call delays
                is_empty_repo=repo_data.get('_is_empty_repo', False)
            )
            repo_data["laborHours"] = round(float(labor_df["EstimatedHours"].sum()), 2) if not labor_df.empty else 0.0
            if repo_data["laborHours"] > 0: logger.info(f"Estimated labor hours for {repo.name}: {repo_data['laborHours']}")

        if cfg_obj:
            repo_data = exemption_processor.process_repository_exemptions(
                repo_data,
                default_org_identifiers=default_ids_for_exemption,
                ai_is_enabled_from_config=cfg_obj.AI_ENABLED_ENV,
                ai_model_name_from_config=cfg_obj.AI_MODEL_NAME_ENV,
                ai_temperature_from_config=cfg_obj.AI_TEMPERATURE_ENV,
                ai_max_output_tokens_from_config=cfg_obj.AI_MAX_OUTPUT_TOKENS_ENV,
                ai_max_input_tokens_from_config=cfg_obj.MAX_TOKENS_ENV
            )
        else:
            logger.warning(
                f"cfg_obj not provided to _process_single_azure_devops_repository for {repo_full_name}. "
                "Exemption processor will use its default AI parameter values."
            )
            repo_data = exemption_processor.process_repository_exemptions(
                repo_data, default_org_identifiers=default_ids_for_exemption
            )
        if inter_repo_adaptive_delay_seconds > 0: # This is the inter-repository adaptive delay
            logger.debug(f"Azure DevOps repo {repo_full_name}: Applying INTER-REPO adaptive delay of {inter_repo_adaptive_delay_seconds:.2f}s")
            time.sleep(inter_repo_adaptive_delay_seconds)

        return repo_data

    except AzureDevOpsServiceError as ado_err_repo:
        logger.error(f"Azure DevOps API error processing repo {repo.name} in {project_name}: {ado_err_repo}. Skipping.", exc_info=False)
        return {"name": repo.name, "organization": organization_name, "_azure_project_name": project_name, "processing_error": f"Azure DevOps API Error: {ado_err_repo}"}
    except Exception as e_repo:
        logger.error(f"Unexpected error processing repo {repo.name} in {project_name}: {e_repo}. Skipping.", exc_info=True)
        return {"name": repo.name, "organization": organization_name, "_azure_project_name": project_name, "processing_error": f"Unexpected Error: {e_repo}"}

def _setup_azure_devops_credentials(
    pat_token: Optional[str],
    spn_client_id: Optional[str],
    spn_client_secret: Optional[str],
    spn_tenant_id: Optional[str],
    logger_instance: logging.Logger
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
        if not ServicePrincipalCredentials:
            logger_instance.error("ServicePrincipalCredentials class not available. Cannot use SPN auth.")
            return None, ""
        credentials = ServicePrincipalCredentials(
            client=spn_client_id, secret=spn_client_secret, tenant=spn_tenant_id, resource=AZURE_DEVOPS_RESOURCE_ID
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

def fetch_repositories(
    token: Optional[str],
    target_path: str,
    processed_counter: List[int],
    processed_counter_lock: threading.Lock,
    debug_limit: int | None = None,
    azure_devops_url: str | None = None,
    hours_per_commit: Optional[float] = None,
    max_workers: int = 5,  # Ensure this parameter exists
    cfg_obj: Optional[Any] = None,
    previous_scan_output_file: Optional[str] = None,
    spn_client_id: Optional[str] = None, # Renamed for clarity to match internal usage
    spn_client_secret: Optional[str] = None, # Renamed
    spn_tenant_id: Optional[str] = None # Renamed
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
    logger.info(f"Attempting to fetch repositories CONCURRENTLY for Azure DevOps project: {organization_name}/{project_name} (max_workers: {max_workers})")
    if not AZURE_SDK_AVAILABLE:
        logger.error("Azure DevOps SDK not available. Skipping Azure DevOps scan.")
        return []
    
    if '/' not in target_path:
        logger.error(f"Invalid Azure DevOps target_path format: '{target_path}'. Expected 'organization/project'.")
        return []
    organization_name, project_name = target_path.split('/', 1)

    # Parse the REPOS_CREATED_AFTER_DATE from cfg_obj
    repos_created_after_filter_date: Optional[datetime] = None
    if cfg_obj and hasattr(cfg_obj, 'REPOS_CREATED_AFTER_DATE'):
        repos_created_after_filter_date = parse_repos_created_after_date(cfg_obj.REPOS_CREATED_AFTER_DATE, logger)

    # --- Load Previous Scan Data for Caching ---
    previous_scan_cache: Dict[str, Dict] = {}
    if previous_scan_output_file:
        logger.info(f"Attempting to load previous Azure DevOps scan data for '{organization_name}/{project_name}' from: {previous_scan_output_file}")
        previous_scan_cache = load_previous_scan_data(previous_scan_output_file, "azure")
    else:
        logger.info(f"No previous scan output file provided for Azure DevOps target '{organization_name}/{project_name}'. Full scan for all repos in this target.")

    azure_devops_api_url = os.getenv("AZURE_DEVOPS_API_URL", "https://dev.azure.com").strip('/')
    organization_url = f"{azure_devops_api_url}/{organization_name}"

    processed_repo_list: List[Dict[str, Any]] = []

    try:
        credentials, auth_method = _setup_azure_devops_credentials(
            pat_token=token, # Assuming 'token' here is the PAT if SPN is not used
            spn_client_id=spn_client_id,
            spn_client_secret=spn_client_secret,
            spn_tenant_id=spn_tenant_id,
            logger_instance=logger
        )
        if not credentials:
            return []

        connection = Connection(base_url=organization_url, creds=credentials)
        # --- SSL Verification Control ---
        disable_ssl_env = os.getenv("DISABLE_SSL_VERIFICATION", "false").lower()
        if disable_ssl_env == "true":
            connection.session.verify = False # Disable SSL verification for the requests session
            logger.warning(f"{ANSI_RED}SECURITY WARNING: SSL certificate verification is DISABLED for Azure DevOps connections due to DISABLE_SSL_VERIFICATION=true.{ANSI_RESET}")
            logger.warning(f"{ANSI_YELLOW}This should ONLY be used for trusted internal environments. Do NOT use in production with public-facing services.{ANSI_RESET}")
        # --- End SSL Verification Control ---

        git_client: GitClient = connection.clients.get_git_client()
        core_client: CoreClient = connection.clients.get_core_client()

        logger.info(f"Successfully established connection to Azure DevOps organization: {organization_name} using {auth_method}.")

        # --- Determine num_repos_in_target for adaptive delay and dynamic intra-repo delays ---
        num_repos_in_target_for_delay_calc = 0
        inter_repo_adaptive_delay_per_repo = 0.0 # For the delay *between* processing repos
        live_repo_list_materialized = None # To store the live list if fetched for count

        cached_repo_count_for_target = 0
        if previous_scan_cache: # Check if cache was loaded and is not empty
            azure_id_field = PLATFORM_CACHE_CONFIG.get("azure", {}).get("id_field", "repo_id")
            valid_cached_repos = [
                r_data for r_id, r_data in previous_scan_cache.items()
                if isinstance(r_data, dict) and r_data.get(azure_id_field) is not None
            ]
            cached_repo_count_for_target = len(valid_cached_repos)
            if cached_repo_count_for_target > 0:
                logger.info(f"CACHE: Found {cached_repo_count_for_target} valid repos in cache for project '{organization_name}/{project_name}'.")
                num_repos_in_target_for_delay_calc = cached_repo_count_for_target
                logger.info(f"ADAPTIVE DELAY/PROCESSING: Using cached count ({num_repos_in_target_for_delay_calc}) as total items estimate for target project '{organization_name}/{project_name}'.")

        if num_repos_in_target_for_delay_calc == 0: # If cache was empty or not used for count
            try:
                logger.info(f"ADAPTIVE DELAY/PROCESSING: Cache empty or not used for count. Fetching live repository list for project '{organization_name}/{project_name}' to get count.")
                # This is an API call
                all_live_repos_for_project = list(git_client.get_repositories(project=project_name))
                initial_live_count = len(all_live_repos_for_project)
                logger.info(f"ADAPTIVE DELAY/PROCESSING: Fetched {initial_live_count} live repositories for project '{organization_name}/{project_name}' before date filtering.")

                # --- Apply REPOS_CREATED_AFTER_DATE filter to live_repo_list_materialized ---
                if repos_created_after_filter_date and all_live_repos_for_project:
                    filtered_live_repos = []
                    skipped_legacy_count = 0
                    for repo_stub_item in all_live_repos_for_project:
                        project_visibility = repo_stub_item.project.visibility.lower() if repo_stub_item.project and repo_stub_item.project.visibility else "private"
                        is_private_project_repo = project_visibility == "private"

                        if not is_private_project_repo: # Public project repos always pass
                            filtered_live_repos.append(repo_stub_item)
                            continue
                        
                        # Private project repo, check project's last update time
                        modified_at_dt = repo_stub_item.project.last_update_time
                        if modified_at_dt and modified_at_dt.tzinfo is None: # Ensure tz-aware
                            modified_at_dt = modified_at_dt.replace(tzinfo=timezone.utc)
                        
                        if modified_at_dt and modified_at_dt >= repos_created_after_filter_date:
                            filtered_live_repos.append(repo_stub_item)
                        else:
                            skipped_legacy_count += 1
                    live_repo_list_materialized = filtered_live_repos # Update with filtered list
                    if skipped_legacy_count > 0:
                        logger.info(f"Azure DevOps: Skipped {skipped_legacy_count} private project legacy repositories from '{organization_name}/{project_name}' due to REPOS_CREATED_AFTER_DATE filter ('{repos_created_after_filter_date.strftime('%Y-%m-%d')}') before full processing.")
                else:
                    live_repo_list_materialized = all_live_repos_for_project # Use all if no filter or no initial repos

                num_repos_in_target_for_delay_calc = len(live_repo_list_materialized) # Count after filtering
                logger.info(f"ADAPTIVE DELAY/PROCESSING: Using API count of {num_repos_in_target_for_delay_calc} (after date filter) as total items estimate for target project '{organization_name}/{project_name}'.")
            except Exception as e_live_count:
                logger.warning(f"Azure DevOps: Error fetching live repository list for project '{organization_name}/{project_name}' to get count: {e_live_count}. num_repos_in_target_for_delay_calc will be 0.", exc_info=True)
                num_repos_in_target_for_delay_calc = 0 # Fallback

        # --- Calculate inter-repo adaptive delay if enabled ---
        if cfg_obj and cfg_obj.ADAPTIVE_DELAY_ENABLED_ENV and num_repos_in_target_for_delay_calc > 0:
            if num_repos_in_target_for_delay_calc > cfg_obj.ADAPTIVE_DELAY_THRESHOLD_REPOS_ENV:
                excess_repos = num_repos_in_target_for_delay_calc - cfg_obj.ADAPTIVE_DELAY_THRESHOLD_REPOS_ENV
                scale_factor = 1 + (excess_repos / cfg_obj.ADAPTIVE_DELAY_THRESHOLD_REPOS_ENV)
                calculated_delay = cfg_obj.ADAPTIVE_DELAY_BASE_SECONDS_ENV * scale_factor
                inter_repo_adaptive_delay_per_repo = min(calculated_delay, cfg_obj.ADAPTIVE_DELAY_MAX_SECONDS_ENV)
                if inter_repo_adaptive_delay_per_repo > 0:
                    logger.info(f"{ANSI_YELLOW}Azure DevOps: INTER-REPO adaptive delay calculated for '{target_path}': {inter_repo_adaptive_delay_per_repo:.2f}s per repository (based on {num_repos_in_target} repositories, {max_workers} workers).{ANSI_RESET}")
        elif cfg_obj and cfg_obj.ADAPTIVE_DELAY_ENABLED_ENV and num_repos_in_target_for_delay_calc == 0:
            logger.info(f"Azure DevOps: Adaptive delay enabled but num_repos_in_target_for_delay_calc is 0 for project '{organization_name}/{project_name}'. No inter-repo adaptive delay will be applied.")
        elif cfg_obj: # Adaptive delay is configured but disabled
            logger.info(f"Azure DevOps: Adaptive delay is disabled by configuration for project '{organization_name}/{project_name}'.")

        # Calculate dynamic POST-API-CALL delay for metadata calls within this target
        dynamic_post_api_call_delay_seconds = 0.0
        if cfg_obj:
            base_delay = float(getattr(cfg_obj, 'AZURE_DEVOPS_POST_API_CALL_DELAY_SECONDS_ENV', os.getenv("AZURE_DEVOPS_POST_API_CALL_DELAY_SECONDS", "0.0")))
            threshold = int(getattr(cfg_obj, 'DYNAMIC_DELAY_THRESHOLD_REPOS_ENV', os.getenv("DYNAMIC_DELAY_THRESHOLD_REPOS", "100")))
            scale = float(getattr(cfg_obj, 'DYNAMIC_DELAY_SCALE_FACTOR_ENV', os.getenv("DYNAMIC_DELAY_SCALE_FACTOR", "1.5")))
            max_d = float(getattr(cfg_obj, 'DYNAMIC_DELAY_MAX_SECONDS_ENV', os.getenv("DYNAMIC_DELAY_MAX_SECONDS", "1.0")))
            
            dynamic_post_api_call_delay_seconds = calculate_dynamic_delay(
                base_delay_seconds=base_delay,
                num_items=num_repos_in_target_for_delay_calc if num_repos_in_target_for_delay_calc > 0 else None,
                threshold_items=threshold, 
                scale_factor=scale, 
                max_delay_seconds=max_d,
                num_workers=max_workers  # Pass the number of workers
            )
            if dynamic_post_api_call_delay_seconds > 0:
                 logger.info(f"{ANSI_YELLOW}Azure DevOps: DYNAMIC POST-API-CALL delay for metadata in target '{target_path}' set to: {dynamic_post_api_call_delay_seconds:.2f}s (based on {num_repos_in_target} repositories, {max_workers} workers).{ANSI_RESET}")

        repo_count_for_project_submitted = 0
        skipped_by_date_filter_count = 0 # Initialize counter for skipped repos

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_repo_name = {}
            try:
                # Use the materialized list if available (from live count), otherwise get fresh iterator
                repositories_iterator: List[GitRepository] = live_repo_list_materialized if live_repo_list_materialized is not None \
                                                            else git_client.get_repositories(project=project_name)
                if dynamic_post_api_call_delay_seconds > 0 and repositories_iterator and live_repo_list_materialized is None: # Apply delay only if we just fetched the list live
                    logger.debug(f"Azure DevOps applying SYNC post-API call delay (get_repositories list): {dynamic_post_api_call_delay_seconds:.2f}s")
                    time.sleep(dynamic_post_api_call_delay_seconds)

                for repo_stub in repositories_iterator:
                    with processed_counter_lock:
                        if debug_limit is not None and processed_counter[0] >= debug_limit:
                            logger.info(f"Global debug limit ({debug_limit}) reached. Stopping further repository submissions for {organization_name}/{project_name}.")
                            break
                        processed_counter[0] += 1
                    
                    repo_stub_full_name_for_log = f"{organization_name}/{project_name}/{repo_stub.name}"

                    # --- Apply REPOS_CREATED_AFTER_DATE filter ---
                    if repos_created_after_filter_date:
                        # Azure DevOps repo visibility is often tied to project visibility.
                        # repo_stub.project is TeamProjectReference
                        project_visibility = repo_stub.project.visibility.lower() if repo_stub.project and repo_stub.project.visibility else "private"
                        is_private_project_repo = project_visibility == "private"

                        if is_private_project_repo:
                            # Use project's last_update_time as a proxy for repo modification date
                            # Ensure it's timezone-aware (it should be from the SDK)
                            modified_at_dt = repo_stub.project.last_update_time
                            if modified_at_dt and modified_at_dt.tzinfo is None: # Make tz-aware if not already
                                modified_at_dt = modified_at_dt.replace(tzinfo=timezone.utc)
                            
                            modified_match = modified_at_dt and modified_at_dt >= repos_created_after_filter_date

                            if modified_match:
                                modified_at_log_str = modified_at_dt.strftime('%Y-%m-%d %H:%M:%S %Z') if modified_at_dt else 'N/A'
                                log_message_parts = [
                                    f"ADO: Private repo '{repo_stub_full_name_for_log}' included "
                                ]
                                log_message_parts.append(f"due to modified date ({modified_at_log_str }).")
                                logger.info(" ".join(log_message_parts))
                            else:
                                # Skip this private repo
                                with processed_counter_lock:
                                    processed_counter[0] -= 1
                                skipped_by_date_filter_count += 1
                                continue # Skip to the next repository
                    # --- End REPOS_CREATED_AFTER_DATE filter ---

                    # --- Get current commit SHA for caching comparison ---
                    current_commit_sha_for_cache = None
                    try:
                        if repo_stub.size == 0: # Proactive check if size is available and 0
                             logger.info(f"Repo {repo_stub_full_name_for_log} has size 0. Cannot get current commit SHA for caching.")
                        elif repo_stub.default_branch:
                            # This is an API call
                            if dynamic_post_api_call_delay_seconds > 0: # Delay before this critical API call
                                logger.debug(f"Azure DevOps applying SYNC post-API call delay (get_commits for SHA): {dynamic_post_api_call_delay_seconds:.2f}s")
                                time.sleep(dynamic_post_api_call_delay_seconds)
                            
                            search_criteria = {'itemVersion.version': repo_stub.default_branch, '$top': 1}
                            commits = git_client.get_commits(repository_id=repo_stub.id, project=project_name, search_criteria=search_criteria, top=1)
                            if commits:
                                current_commit_sha_for_cache = commits[0].commit_id
                                logger.debug(f"Successfully fetched current commit SHA '{current_commit_sha_for_cache}' for default branch '{repo_stub.default_branch}' of {repo_stub_full_name_for_log}.")
                    except AzureDevOpsServiceError as e_sha_fetch:
                        logger.warning(f"Azure DevOps API error fetching current commit SHA for {repo_stub_full_name_for_log}: {e_sha_fetch}. Proceeding without SHA for caching.")
                    except Exception as e_sha_unexpected:
                        logger.error(f"Unexpected error fetching current commit SHA for {repo_stub_full_name_for_log}: {e_sha_unexpected}. Proceeding without SHA for caching.", exc_info=True)

                    repo_count_for_project_submitted += 1 # Increment for repos submitted to executor
                    future = executor.submit(
                        _process_single_azure_devops_repository,
                        git_client,
                        core_client,
                        repo_stub,
                        organization_name,
                        project_name,
                        pat_token, 
                        spn_client_id, 
                        spn_client_secret,
                        spn_tenant_id,
                        hours_per_commit,
                        cfg_obj,
                        inter_repo_adaptive_delay_per_repo, # Pass inter-repo adaptive delay
                        dynamic_post_api_call_delay_seconds, # Pass dynamic per-API call delay
                        previous_scan_cache=previous_scan_cache, # Pass cache
                        current_commit_sha=current_commit_sha_for_cache, # Pass current SHA
                        num_workers=max_workers
                    )
                    future_to_repo_name[future] = f"{organization_name}/{project_name}/{repo_stub.name}"
            
            except AzureDevOpsServiceError as ado_list_err:
                logger.error(f"Azure DevOps API error during initial repository listing for {organization_name}/{project_name}. Processing submitted tasks. Details: {ado_list_err}")
            except Exception as ex_iter:
                logger.error(f"Unexpected error during initial repository listing for {organization_name}/{project_name}: {ex_iter}. Processing submitted tasks.")

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
                    logger.error(f"Repository {repo_name_for_log} generated an exception in its thread: {exc}", exc_info=True)
                    name_parts = repo_name_for_log.split('/')
                    repo_n = name_parts[-1] if len(name_parts) > 0 else "UnknownRepo"
                    org_n = name_parts[0] if len(name_parts) > 1 else organization_name
                    proj_n = name_parts[1] if len(name_parts) > 2 else project_name

                    processed_repo_list.append({"name": repo_n, 
                                                "organization": org_n, 
                                                "_azure_project_name": proj_n,
                                                "processing_error": f"Thread execution failed: {exc}"})

        logger.info(f"Finished processing for {repo_count_for_project_submitted} repositories from Azure DevOps project: {organization_name}/{project_name}. Collected {len(processed_repo_list)} results.")
        if repos_created_after_filter_date and skipped_by_date_filter_count > 0:
            logger.info(f"Azure DevOps: Skipped {skipped_by_date_filter_count} private project repositories from '{organization_name}/{project_name}' due to the REPOS_CREATED_AFTER_DATE filter ('{repos_created_after_filter_date.strftime('%Y-%m-%d')}').")

    except AzureDevOpsServiceError as e:
        logger.critical(f"Azure DevOps API error for {organization_name}/{project_name} (using {auth_method}): {e}", exc_info=False)
        return [] 
    except Exception as e:
        logger.critical(f"An unexpected error occurred during Azure DevOps connection or processing for {organization_name}/{project_name}: {e}", exc_info=True)
        return []

    return processed_repo_list


if __name__ == '__main__':
    from dotenv import load_dotenv as load_dotenv_for_test 
    load_dotenv_for_test()

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
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
        counter_lock = threading.Lock()
        repositories = fetch_repositories(
            pat_token=test_pat_token,
            target_path=test_target_full_path, # Pass the combined path
            spn_client_id=test_spn_client_id, # Pass SPN details
            spn_client_secret=test_spn_client_secret,
            spn_tenant_id=test_spn_tenant_id,
            processed_counter=counter, 
            processed_counter_lock=counter_lock, 
            debug_limit=None,
            cfg_obj=None, # For this direct test, cfg_obj is None.
            previous_scan_output_file=None # No cache for direct test
        )
        
        if repositories:
            logger.info(f"Successfully fetched {len(repositories)} repositories.")
            for i, repo_info in enumerate(repositories[:3]):
                logger.info(f"--- Repository {i+1} ({repo_info.get('name')}) ---")
                logger.info(f"  Repo ID: {repo_info.get('repo_id')}")
                if "processing_error" in repo_info:
                    logger.error(f"  Processing Error: {repo_info['processing_error']}")
            if len(repositories) > 3:
                logger.info(f"... and {len(repositories)-3} more repositories.")
        else:
            logger.warning("No repositories fetched or an error occurred.")
        logger.info(f"Total repositories processed according to counter: {counter[0]}")
