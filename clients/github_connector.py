# clients\github_connector.py
"""
GitHub Connector for Share IT Act Repository Scanning Tool.

This module is responsible for fetching repository data from GitHub,
including metadata, README content, CODEOWNERS files, topics, and tags.
It interacts with the GitHub API via the PyGithub library.
"""

import os
import logging
import base64 # For decoding README content
from typing import List, Dict, Optional, Any
from datetime import timezone
# Removed: from dotenv import load_dotenv - No longer needed here for auth

from github import Github, GithubException, UnknownObjectException, RateLimitExceededException

# Attempt to import the exemption processor
try:
    from utils import exemption_processor
except ImportError:
    logging.getLogger(__name__).error(
        "Failed to import exemption_processor from utils. "
        "Exemption processing will be skipped by the GitHub connector (using mock)."
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

PLACEHOLDER_GITHUB_TOKEN = "YOUR_GITHUB_PAT"

def is_placeholder_token(token: Optional[str]) -> bool:
    """Checks if the GitHub token is missing or a known placeholder."""
    return not token or token == PLACEHOLDER_GITHUB_TOKEN

def _get_readme_details_pygithub(repo_obj) -> tuple[Optional[str], Optional[str]]:
    """
    Fetches and decodes the README content and its HTML URL.
    Tries common README filenames.
    """
    common_readme_names = ["README.md", "README.txt", "README", "readme.md"]
    for readme_name in common_readme_names:
        try:
            readme_file = repo_obj.get_contents(readme_name)
            readme_content_bytes = base64.b64decode(readme_file.content)
            try:
                readme_content_str = readme_content_bytes.decode('utf-8')
            except UnicodeDecodeError:
                try:
                    readme_content_str = readme_content_bytes.decode('latin-1')
                except Exception:
                    readme_content_str = readme_content_bytes.decode('utf-8', errors='ignore')
            
            readme_url = readme_file.html_url 
            logger.debug(f"Successfully fetched README '{readme_name}' (URL: {readme_url}) for {repo_obj.full_name}")
            return readme_content_str, readme_url
        except UnknownObjectException:
            logger.debug(f"README '{readme_name}' not found in {repo_obj.full_name}")
            continue
        except GithubException as e:
            logger.error(f"GitHub API error fetching README '{readme_name}' for {repo_obj.full_name}: {e.status} {getattr(e, 'data', str(e))}", exc_info=False)
            return None, None # Stop if other API error
        except Exception as e:
            logger.error(f"Unexpected error decoding README '{readme_name}' for {repo_obj.full_name}: {e}", exc_info=True)
            return None, None
    logger.debug(f"No common README file found for {repo_obj.full_name}")
    return None, None


def _get_codeowners_content_pygithub(repo_obj) -> Optional[str]:
    codeowners_locations = ["CODEOWNERS", ".github/CODEOWNERS", "docs/CODEOWNERS"]
    for location in codeowners_locations:
        try:
            codeowners_file = repo_obj.get_contents(location)
            codeowners_content = codeowners_file.decoded_content.decode('utf-8', errors='replace')
            logger.debug(f"Successfully fetched CODEOWNERS from '{location}' for {repo_obj.full_name}")
            return codeowners_content
        except UnknownObjectException:
            logger.debug(f"CODEOWNERS file not found at '{location}' in {repo_obj.full_name}")
            continue
        except GithubException as e:
            logger.error(f"GitHub API error fetching CODEOWNERS from '{location}' for {repo_obj.full_name}: {e.status} {getattr(e, 'data', str(e))}", exc_info=False)
            return None # Stop if other API error
        except Exception as e:
            logger.error(f"Unexpected error decoding CODEOWNERS from '{location}' for {repo_obj.full_name}: {e}", exc_info=True)
            return None
    logger.debug(f"No CODEOWNERS file found in standard locations for {repo_obj.full_name}")
    return None


def _fetch_tags_pygithub(repo_obj) -> List[str]:
    tag_names = []
    try:
        logger.debug(f"Fetching tags for repo: {repo_obj.full_name}")
        tags = repo_obj.get_tags()
        tag_names = [tag.name for tag in tags if tag.name]
        logger.debug(f"Found {len(tag_names)} tags for {repo_obj.full_name}")
    except RateLimitExceededException:
        logger.error(f"Rate limit exceeded while fetching tags for {repo_obj.full_name}. Skipping tags for this repo.")
    except GithubException as e:
        logger.error(f"GitHub API error fetching tags for {repo_obj.full_name}: {e.status} {getattr(e, 'data', str(e))}", exc_info=False)
    except Exception as e: # Catch other potential errors like network issues during this specific call
        logger.error(f"Unexpected error fetching tags for {repo_obj.full_name}: {e}", exc_info=True)
    return tag_names


def fetch_repositories(
    token: Optional[str], 
    org_name: str, 
    processed_counter: List[int], 
    debug_limit: Optional[int], 
    github_instance_url: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Fetches repository details from a specific GitHub organization.

    Args:
        token: The GitHub Personal Access Token.
        org_name: The name of the GitHub organization.
        processed_counter: Mutable list to track processed repositories for debug limit.
        debug_limit: Optional global limit for repositories to process.
        github_instance_url: Optional base URL for GitHub Enterprise Server.

    Returns:
        A list of dictionaries, each containing processed metadata for a repository.
    """
    instance_msg = f"GitHub instance: {github_instance_url}" if github_instance_url else "public GitHub.com"
    logger.info(f"Attempting to fetch repositories for GitHub organization: {org_name} on {instance_msg}")

    if is_placeholder_token(token): # is_placeholder_token now takes token as arg
        logger.error("GitHub token is a placeholder or missing. Cannot fetch repositories.")
        return []

    try:
        if github_instance_url:
            # For GitHub Enterprise, the base_url should be the API endpoint
            # e.g., https://hostname/api/v3
            if not github_instance_url.endswith("/api/v3"):
                base_url = github_instance_url.rstrip('/') + "/api/v3"
            else:
                base_url = github_instance_url
            gh = Github(base_url=base_url, login_or_token=token)
            logger.info(f"Connecting to GitHub Enterprise Server at {base_url}")
        else:
            gh = Github(login_or_token=token) # Default to public GitHub.com
            logger.info("Connecting to public GitHub.com")
        
        organization = gh.get_organization(org_name)
        logger.info(f"Successfully connected to GitHub instance and found organization: {org_name}")
    except RateLimitExceededException as rle:
        logger.critical(f"GitHub API rate limit exceeded when trying to connect or get org '{org_name}'. Cannot proceed. Details: {rle}")
        return []
    except UnknownObjectException:
        logger.error(f"GitHub organization '{org_name}' not found on {instance_msg} or token lacks permissions.")
        return []
    except GithubException as e:
        logger.critical(f"Failed to connect to GitHub or get organization '{org_name}' on {instance_msg}: {e.status} {getattr(e, 'data', str(e))}", exc_info=False)
        return []
    except Exception as e: # Catch other potential errors like network resolution for GHES URL
        logger.critical(f"An unexpected error occurred initializing GitHub connection for org '{org_name}' on {instance_msg}: {e}", exc_info=True)
        return []

    processed_repo_list: List[Dict[str, Any]] = []
    try:
        repos_iterator = organization.get_repos(type='all') # type can be 'all', 'public', 'private', 'forks', 'sources', 'member'
        repo_count_for_org = 0
        for repo in repos_iterator:
            if debug_limit is not None and processed_counter[0] >= debug_limit:
                logger.info(f"Global debug limit ({debug_limit}) reached. Stopping repository fetching for {org_name}.")
                break

            repo_full_name = repo.full_name
            logger.info(f"Processing repository: {repo_full_name}")
            repo_count_for_org += 1
            
            repo_data: Dict[str, Any] = {} 
            try:
                if repo.fork:
                    logger.info(f"Skipping forked repository: {repo_full_name}")
                    continue

                created_at_dt = repo.created_at.replace(tzinfo=timezone.utc) if repo.created_at else None
                pushed_at_dt = repo.pushed_at.replace(tzinfo=timezone.utc) if repo.pushed_at else None 
                updated_at_dt = repo.updated_at.replace(tzinfo=timezone.utc) if repo.updated_at else None

                repo_visibility = "public" 
                if repo.private:
                    repo_visibility = "private"
                # PyGithub's repo.visibility attribute is more explicit if available (GHES might differ)
                if hasattr(repo, 'visibility') and repo.visibility: 
                    if repo.visibility.lower() in ["public", "private", "internal"]:
                         repo_visibility = repo.visibility.lower()

                all_languages_list = []
                try:
                    languages_dict = repo.get_languages()
                    if languages_dict:
                        all_languages_list = list(languages_dict.keys())
                except Exception as lang_err: # Catch broad errors, e.g. if repo is empty
                    logger.warning(f"Could not fetch languages for {repo_full_name}: {lang_err}", exc_info=False)

                licenses_list = []
                if repo.license and hasattr(repo.license, 'spdx_id') and repo.license.spdx_id and repo.license.spdx_id.lower() != "noassertion":
                    license_entry = {"spdxID": repo.license.spdx_id}
                    # PyGithub license object doesn't always have a direct html_url for the license text itself.
                    # The key (often spdx_id) is the most reliable.
                    if hasattr(repo.license, 'html_url') and repo.license.html_url: # This is usually API URL to license
                        pass # Not adding this as it's not the license text URL
                    if hasattr(repo.license, 'name') and repo.license.name:
                        license_entry["name"] = repo.license.name
                    licenses_list.append(license_entry)
                
                readme_content_str, readme_html_url = _get_readme_details_pygithub(repo)
                codeowners_content_str = _get_codeowners_content_pygithub(repo)
                repo_topics = repo.get_topics() # List of strings
                repo_git_tags = _fetch_tags_pygithub(repo) # List of strings (tag names)

                repo_data = {
                    "name": repo.name,
                    "organization": org_name, 
                    "description": repo.description or "",
                    "repositoryURL": repo.html_url,
                    "homepageURL": repo.homepage or "", 
                    "downloadURL": None, 
                    "vcs": "git",
                    "repositoryVisibility": repo_visibility,
                    "status": "development", 
                    "version": "N/A",      
                    "laborHours": 0,       
                    "languages": all_languages_list,
                    "tags": repo_topics,
                    "date": {
                        "created": created_at_dt.isoformat() if created_at_dt else None,
                        "lastModified": pushed_at_dt.isoformat() if pushed_at_dt else (updated_at_dt.isoformat() if updated_at_dt else None),
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
                    "archived": repo.archived,  
                }
                
                # Pass default identifiers for organization context to exemption_processor
                repo_data = exemption_processor.process_repository_exemptions(repo_data, default_org_identifiers=[org_name])
                
                processed_repo_list.append(repo_data)
                processed_counter[0] += 1

            except RateLimitExceededException as rle_repo:
                logger.error(f"GitHub API rate limit exceeded processing repo {repo_full_name}. Skipping remaining for {org_name}. Details: {rle_repo}")
                break 
            except GithubException as gh_err_repo:
                logger.error(f"GitHub API error processing repo {repo_full_name}: {gh_err_repo.status} {getattr(gh_err_repo, 'data', str(gh_err_repo))}. Skipping.", exc_info=False)
                processed_repo_list.append({"name": repo.name, "organization": org_name, "processing_error": f"GitHub API Error: {gh_err_repo.status}"})
            except Exception as e_repo:
                logger.error(f"Unexpected error processing repo {repo_full_name}: {e_repo}. Skipping.", exc_info=True)
                processed_repo_list.append({"name": repo.name, "organization": org_name, "processing_error": f"Unexpected Error: {e_repo}"})

        logger.info(f"Fetched and initiated processing for {repo_count_for_org} repositories from GitHub organization: {org_name}")

    except RateLimitExceededException as rle_org_iteration:
        logger.error(f"GitHub API rate limit exceeded iterating repositories for {org_name}. Partial results may be returned. Details: {rle_org_iteration}")
    except GithubException as gh_err_org_iteration:
        logger.error(f"GitHub API error iterating repositories for {org_name}: {gh_err_org_iteration.status} {getattr(gh_err_org_iteration, 'data', str(gh_err_org_iteration))}. Partial results.", exc_info=False)
    except Exception as e_org_iteration: # Catch other potential errors like network resolution for GHES URL
        logger.error(f"Unexpected error iterating repositories for {org_name} on {instance_msg}: {e_org_iteration}. Partial results.", exc_info=True)

    return processed_repo_list

if __name__ == '__main__':
    # This basic test block will use environment variables for token and org
    # To test GHES, you'd need to set GITHUB_ENTERPRISE_URL in .env or modify this test
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # For testing, you'd typically get these from CLI args or a test config
    # For this direct run, we'll simulate getting them (e.g. from env for simplicity)
    test_gh_token = os.getenv("GITHUB_TOKEN_TEST") # Use a specific test token if needed
    test_org_name_env = os.getenv("GITHUB_ORGS_TEST", "").split(',')[0].strip() # Test with first org from a test env var
    test_ghes_url_env = os.getenv("GITHUB_ENTERPRISE_URL_TEST")


    if not test_gh_token or is_placeholder_token(test_gh_token):
        logger.error("Test GitHub token (GITHUB_TOKEN_TEST) not found or is a placeholder in .env.")
    elif not test_org_name_env:
        logger.error("No GitHub organization found in GITHUB_ORGS_TEST in .env for testing.")
    else:
        instance_for_test = test_ghes_url_env or "public GitHub.com"
        logger.info(f"--- Testing GitHub Connector for organization: {test_org_name_env} on instance: {instance_for_test} ---")
        counter = [0]
        
        repositories = fetch_repositories(
            token=test_gh_token, 
            org_name=test_org_name_env, 
            processed_counter=counter, 
            debug_limit=None, 
            github_instance_url=test_ghes_url_env
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
