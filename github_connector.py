# github_connector.py
import os
import logging
from datetime import datetime, timezone # Added timezone import
from github import Github, BadCredentialsException, UnknownObjectException, GithubException
from requests.exceptions import RequestException # Keep requests for potential direct API calls if needed
import base64
from typing import List, Optional, Dict, Any # Added typing
import requests # Need requests for _fetch_paginated_data if using direct calls
from urllib.parse import urlparse, urlunparse # For pagination helper

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
    for path in common_paths:
        try:
            # Use the PyGithub object's get_contents method
            content_file = repo.get_contents(path)
            content_bytes = base64.b64decode(content_file.content)
            try:
                return content_bytes.decode('utf-8')
            except UnicodeDecodeError:
                logger.warning(f"Could not decode CODEOWNERS at {path} as UTF-8 for {repo.name}. Trying latin-1.")
                try:
                    return content_bytes.decode('latin-1')
                except Exception:
                     logger.error(f"Failed to decode CODEOWNERS at {path} for {repo.name} even with latin-1.")
                     return None # Give up if decode fails
        except UnknownObjectException:
            continue # Try next path
        except Exception as e:
            # Log other errors like potential permission issues or API errors
            logger.error(f"Error fetching CODEOWNERS at {path} for {repo.name}: {e}", exc_info=True)
            # Don't stop processing the repo, just skip CODEOWNERS
    logger.debug(f"No CODEOWNERS file found in standard locations for {repo.name}")
    return None
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
    # --- Import the processor ---
    # Import locally to avoid potential circular dependencies if exemption_processor imports connectors
    try:
        import exemption_processor
    except ImportError:
        logger.critical("Failed to import exemption_processor. Cannot proceed.")
        return []


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
                repo_language = repo.language

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
                    "languages": [repo_language] if repo_language else [],
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
                    "_language_heuristic": repo_language,

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
                processed_data.pop('_language_heuristic', None)
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
