# github_connector.py
import os
import logging
import json 
from dotenv import load_dotenv 
from datetime import datetime, timezone 
from github import Github, BadCredentialsException, UnknownObjectException, GithubException
from requests.exceptions import RequestException # Keep requests for potential direct API calls if needed
import base64
from typing import List, Optional, Dict, Any 
import requests # Need requests for _fetch_paginated_data if using direct calls
from urllib.parse import urlparse, urlunparse # For pagination helper
# --- Import the processor ---
import utils.exemption_processor

logger = logging.getLogger(__name__)

DEFAULT_PER_PAGE = 100 # Default number of items per page for GitHub API

def is_placeholder_token(token):
    """Checks if the token is missing or likely a placeholder."""
    return not token or (token.startswith("ghp_") and len(token) < 40)

# --- Helper to fetch CODEOWNERS ---
def _get_codeowners_content(repo) -> Optional[str]:
    """Fetches CODEOWNERS content from standard locations using PyGithub object."""
    common_paths = [
        ".github/CODEOWNERS",
        "docs/CODEOWNERS",
        "CODEOWNERS"
    ]
    repo_full_name = getattr(repo, 'full_name', 'UnknownRepo') # Get repo name safely for logging

    for path in common_paths:
        try:
            # Use the PyGithub object's get_contents method
            logger.debug(f"Attempting to fetch CODEOWNERS at path: '{path}' for repo: {repo_full_name}")
            content_file = repo.get_contents(path)
            content_bytes = base64.b64decode(content_file.content)
            try:
                # Try decoding as UTF-8 first
                return content_bytes.decode('utf-8')
            except UnicodeDecodeError:
                # If UTF-8 fails, try latin-1 as a fallback
                logger.warning(f"Could not decode CODEOWNERS at {path} as UTF-8 for {repo_full_name}. Trying latin-1.")
                try:
                    return content_bytes.decode('latin-1')
                except Exception as decode_err:
                     # If latin-1 also fails, log error and return None for this path
                     logger.error(f"Failed to decode CODEOWNERS at {path} for {repo_full_name} even with latin-1: {decode_err}")
                     # Returning None here means we might find it at another path, which is okay.
                     # If this was the last path, the function will return None overall.
                     return None
        except UnknownObjectException:
            # This means the file was not found at this specific path (404)
            logger.debug(f"CODEOWNERS file not found at path: '{path}' for repo: {repo_full_name}")
            continue # Try the next common path

        # --- MODIFIED: Catch specific PyGithub API errors ---
        except GithubException as ge:
            # Catches API errors like rate limits (403), server errors (5xx), etc.
            status = getattr(ge, 'status', 'N/A')
            message = getattr(ge, 'data', {}).get('message', str(ge)) # Try to get specific message
            logger.error(f"GitHub API error (Status: {status}) fetching CODEOWNERS at '{path}' for {repo_full_name}: {message}", exc_info=False) # Log concise error
            # Optional: Log full traceback only in DEBUG level if needed
            # logger.debug(f"Full traceback for GithubException fetching {path} in {repo_full_name}:", exc_info=True)

            # If it's a rate limit (403) or server error (5xx), it's unlikely subsequent paths will work for this repo now.
            # Stop trying to find CODEOWNERS for this specific repository to avoid further errors/rate limit issues.
            if status == 403 or status >= 500:
                 logger.warning(f"Stopping CODEOWNERS check for {repo_full_name} due to API error status {status}.")
                 return None # Give up on finding CODEOWNERS for this repo entirely

            # For other GithubExceptions (e.g., maybe a specific permission issue on this path),
            # continue to the next path just in case.
            continue

        except Exception as e:
            # Catch any other unexpected errors during the process for this specific path
            logger.error(f"Unexpected error fetching/processing CODEOWNERS at path '{path}' for {repo_full_name}: {e}", exc_info=True)
            # Continue to the next path, as the error might be specific to this attempt/path
            continue # Try next path

    # If the loop completes without finding the file or returning early due to error
    logger.debug(f"No CODEOWNERS file found in standard locations for {repo_full_name}")
# --- END Helper ---

# --- REMOVED _fetch_paginated_data and _fetch_tags_pygithub as api_tags are no longer needed ---

def fetch_repositories(token, org_name, processed_counter: list[int], debug_limit: int | None) -> list[dict]:
    """
    Fetches repository details from GitHub, processes exemptions,
    respecting a global limit, and returns a list of processed repository data dictionaries.

    Args:
        token: GitHub PAT.
        org_name: GitHub organization name.
        processed_counter: A mutable list containing the current global count of processed repos.
        debug_limit: The maximum number of repos to process globally (or None).
    """


    if is_placeholder_token(token):
        logger.info("GitHub token is missing or appears to be a placeholder. Skipping GitHub scan.")
        return []
    if not org_name:
        logger.warning("GitHub organization name not provided. Skipping GitHub scan.")
        return []

    processed_repo_list = [] # Store final processed data
    g = None
    try:
        logger.info(f"Attempting to connect to GitHub API...")
        # Use PyGithub library
        g = Github(login_or_token=token, timeout=30) # Add timeout
        user = g.get_user()
        logger.info(f"GitHub SDK initialized and authenticated as user: {user.login}")

        logger.info(f"Fetching repositories for GitHub organization: {org_name} ..")
        org = g.get_organization(org_name)
        # get_repos returns a PaginatedList, which handles pagination automatically
        repos = org.get_repos(type='all')

        # --- Loop through repos, respecting the limit ---
        for i, repo in enumerate(repos): # Iterating through PaginatedList
            # --- ADD DEBUG CHECK ---
            if debug_limit is not None and processed_counter[0] >= debug_limit:
                logger.warning(f"--- DEBUG MODE: Global limit ({debug_limit}) reached during GitHub scan. Stopping GitHub fetch. ---")
                break # Exit the loop over GitHub repos
            # --- END DEBUG CHECK ---

            repo_data = {} # Start with an empty dict for this repo
            try:
               # --- Add fork check early ---
                if repo.fork:
                    logger.info(f"Skipping forked repository: {repo.full_name}")
                    continue # Move to the next repository in the loop
                # --- End fork check ---

                logger.debug(f"Fetching data for GitHub repo: {repo.full_name}")
                # --- Fetch Base Data ---
                created_at_iso = repo.created_at.isoformat() if repo.created_at else None
                pushed_at_iso = repo.pushed_at.isoformat() if repo.pushed_at else None
                repo_visibility = "private" if repo.private else "public"
                # repo_language = repo.language # Keep primary if needed elsewhere, but fetch all
                # --- Fetch ALL Languages ---
                all_languages_list = []
                try:
                    languages_dict = repo.get_languages() # Returns dict like {'Python': 123, 'HTML': 45}
                    if languages_dict:
                        all_languages_list = list(languages_dict.keys())
                        logger.debug(f"Fetched languages for {repo.full_name}: {all_languages_list}")
                    else:
                        logger.debug(f"No languages detected by API for {repo.full_name}")
                except Exception as lang_err:
                    logger.error(f"Error fetching languages for {repo.full_name}: {lang_err}", exc_info=True)

                # Prepare license structure
                licenses_list = []
                if repo.license:
                    licenses_list.append({
                        "name": repo.license.name,
                        # "URL": repo.license.url # PyGithub might provide the API URL for the license details
                    })
                # Add default license if none found
                if not licenses_list:
                    logger.debug(f"No license found via API for {repo.name}. Applying default: Apache License 2.0")
                    licenses_list.append({
                        "name": "Apache License 2.0",
                        "URL": "https://www.apache.org/licenses/LICENSE-2.0"
                    })

                # --- Fetch README Content ---
                readme_content_str: Optional[str] = None
                readme_url: Optional[str] = None
                try:
                    readme_file = repo.get_readme()
                    readme_url = readme_file.html_url
                    readme_content_bytes = base64.b64decode(readme_file.content)
                    try: readme_content_str = readme_content_bytes.decode('utf-8')
                    except UnicodeDecodeError:
                        try: readme_content_str = readme_content_bytes.decode('latin-1')
                        except Exception: readme_content_str = readme_content_bytes.decode('utf-8', errors='ignore')
                    logger.debug(f"Successfully fetched README for {repo.name}")
                except UnknownObjectException: logger.debug(f"No README found for repository: {repo.name}")
                except Exception as readme_err: logger.error(f"Error fetching/decoding README for {repo.name}: {readme_err}", exc_info=True)

                # --- Fetch CODEOWNERS Content ---
                codeowners_content_str = _get_codeowners_content(repo)

                # --- Fetch Topics ---
                repo_topics = repo.get_topics() # PyGithub method to get topics

                # --- REMOVED Fetching Tags (api_tags) ---
                # repo_tags = _fetch_tags_pygithub(repo)

                # Build the dictionary using the structure you provided
                repo_data = {
                    # === Core Schema Fields ===
                    "name": repo.name,
                    "description": repo.description or '',
                    "organization": repo.owner.login,
                    "repositoryURL": repo.html_url,
                    "homepageURL": repo.homepage or repo.html_url, # Use actual homepage if available
                    "downloadURL": None, # Keep as None unless specific release logic is added
                    "vcs": "git",
                    "repositoryVisibility": repo_visibility,
                    "status": "development", # Placeholder - exemption_processor might update based on README
                    "version": "N/A", # Placeholder - exemption_processor might update based on README
                    "laborHours": 0, # Placeholder
                  #  "languages": [repo_language] if repo_language else [],
                    "languages": all_languages_list, # Populate with the full list
                    "tags": repo_topics, # Use fetched topics directly for the 'tags' field

                    # === Nested Schema Fields ===
                    "date": {
                        "created": created_at_iso,
                        "lastModified": pushed_at_iso,
                    },
                    "permissions": {
                        "usageType": None,
                        "exemptionText": None,
                        "licenses": licenses_list
                    },
                    "contact": {
                        "name": "Centers for Disease Control and Prevention",
                        "email": None
                    },
                    "contractNumber": None,

                    # === Fields needed for processing (will be removed later) ===
                    "readme_content": readme_content_str,
                    "_codeowners_content": codeowners_content_str,
                    "_is_private_flag": repo.private,
                    "_all_languages": all_languages_list, # Pass the full list

                    # === Additional Fields (Useful for Inference/Debugging) ===
                    "repo_id": repo.id,
                    "readme_url": readme_url,
                    # "api_tags": repo_tags, # REMOVED
                    "archived": repo.archived, # Store archived status for status inference
                }

                # --- Call Exemption Processor ---
                processed_data = exemption_processor.process_repository_exemptions(repo_data)

                # --- Clean up temporary/processed fields ---
                # Processor now handles removing readme_content and _codeowners_content
                processed_data.pop('_is_private_flag', None)
                #processed_data.pop('_language_heuristic', None)
                processed_data.pop('_all_languages', None) 
                # Keep 'archived' if needed by generate_codejson.py inference functions
                # processed_data.pop('archived', None)

                # Add the fully processed data to the list
                processed_repo_list.append(processed_data)

                # --- INCREMENT GLOBAL COUNTER ---
                processed_counter[0] += 1
                # --- END INCREMENT ---

            except Exception as repo_err:
                logger.error(f"Error processing GitHub repository '{repo.name}' (within main loop): {repo_err}", exc_info=True)
                processed_repo_list.append({
                    'name': repo.name,
                    'organization': repo.owner.login,
                    'processing_error': f"Connector stage: {repo_err}"
                 })
                processed_counter[0] += 1


        logger.info(f"Finished GitHub scan. Processed {len(processed_repo_list)} repositories in this connector. Global count: {processed_counter[0]}")

    # --- Exception Handling ---
    except BadCredentialsException:
        logger.error(f"GitHub authentication failed. Check GITHUB_TOKEN. Skipping.")
        return []
    except UnknownObjectException:
        logger.error(f"GitHub organization '{org_name}' not found. Check GITHUB_ORG. Skipping.")
        return []
    except GithubException as e:
        message = e.data.get('message', '') if isinstance(e.data, dict) else str(e.data)
        logger.error(f"GitHub API error: {e.status} {message}. Skipping.", exc_info=True)
        return []
    except RequestException as e:
        logger.error(f"Network error connecting to GitHub: {e}. Skipping.", exc_info=True)
        return []
    except Exception as e:
        logger.error(f"Unexpected error during GitHub fetch for org '{org_name}': {e}. Skipping.", exc_info=True)
        return []

    return processed_repo_list



if __name__ == "__main__":
    # --- Added Setup Code ---
    # Load .env file for standalone execution
    load_dotenv()

    # Basic logging setup for direct execution
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__) # Use the connector's logger

    logger.info("Running GitHub connector directly for testing...")

    # Get necessary config from environment
    github_token = os.getenv("GITHUB_TOKEN")
    github_org = os.getenv("GITHUB_ORG")
    # --- End Added Setup Code ---

    if not github_token or not github_org:
        logger.error("GITHUB_TOKEN and GITHUB_ORG must be set in .env file for direct execution.")
    else:
        try:
            # --- Set a specific limit for direct testing ---
            test_counter = [0]
            # Set a small limit, e.g., 5 repositories
            test_limit = 5
            # --- End limit setting ---

            logger.info(f"Fetching repositories for org: {github_org} (Limit: {test_limit})") # Log the limit

            repositories = fetch_repositories(
                token=github_token,
                org_name=github_org,
                processed_counter=test_counter,
                debug_limit=test_limit # Pass the limit here
            )

            logger.info(f"Direct execution finished. Found {len(repositories)} repositories (up to limit).")

            # Print the results nicely formatted as JSON
            print("\n--- Fetched Repositories (JSON Output) ---")
            # Use default=str to handle potential non-serializable types like datetime
            print(json.dumps(repositories, indent=2, default=str))
            print("--- End of Output ---")

        except Exception as e:
            logger.error(f"An error occurred during direct execution: {e}", exc_info=True)

    logger.info("Direct execution script finished.")
# --- End of block ---
