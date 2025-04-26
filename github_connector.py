# github_connector.py
import os
import logging
from datetime import datetime
from github import Github, BadCredentialsException, UnknownObjectException, GithubException
from requests.exceptions import RequestException
import base64
from typing import List, Optional, Dict, Any # Added typing

logger = logging.getLogger(__name__)

def is_placeholder_token(token):
    """Checks if the token is missing or likely a placeholder."""
    return not token or (token.startswith("ghp_") and len(token) < 40)

# --- ADD Helper to fetch CODEOWNERS ---
def _get_codeowners_content(repo) -> Optional[str]:
    """Fetches CODEOWNERS content from standard locations."""
    common_paths = [
        ".github/CODEOWNERS",
        "docs/CODEOWNERS",
        "CODEOWNERS"
    ]
    for path in common_paths:
        try:
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
            logger.error(f"Error fetching CODEOWNERS at {path} for {repo.name}: {e}", exc_info=True)
            # Don't stop processing the repo, just skip CODEOWNERS
    logger.debug(f"No CODEOWNERS file found in standard locations for {repo.name}")
    return None
# --- END Helper ---


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
    # --- Import the new processor ---
    import exemption_processor

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
        g = Github(login_or_token=token)
        user = g.get_user()
        logger.info(f"GitHub SDK initialized and authenticated as user: {user.login}")

        logger.info(f"Fetching repositories for GitHub organization: {org_name} ..")
        org = g.get_organization(org_name)
        repos = org.get_repos(type='all')

        # --- Loop through repos, respecting the limit ---
        for i, repo in enumerate(repos):
            # --- ADD DEBUG CHECK ---
            if debug_limit is not None and processed_counter[0] >= debug_limit:
                logger.warning(f"--- DEBUG MODE: Global limit ({debug_limit}) reached during GitHub scan. Stopping GitHub fetch. ---")
                break # Exit the loop over GitHub repos
            # --- END DEBUG CHECK ---

            repo_data = {} # Start with an empty dict for this repo
            try:
               # --- Add fork check early ---
                if repo.fork:
                    # Log at INFO level so it appears in the terminal by default
                    logger.info(f"Skipping forked repository: {repo.full_name}")
                    continue # Move to the next repository in the loop
                # --- End fork check ---

                logger.debug(f"Fetching data for GitHub repo: {repo.full_name}")
                # --- Fetch Base Data ---
                # --- Prepare Base Data and Initial Schema Structure ---
                created_at_iso = repo.created_at.isoformat() if repo.created_at else None
                # updated_at_iso = repo.updated_at.isoformat() if repo.updated_at else None # Not used directly
                pushed_at_iso = repo.pushed_at.isoformat() if repo.pushed_at else None
                repo_visibility = "private" if repo.private else "public"
                repo_language = repo.language # Get primary language

                # Prepare license structure (Schema requires a list)
                licenses_list = []
                if repo.license:
                    licenses_list.append({
                        "name": repo.license.name,
                        # "URL": None # Placeholder - API doesn't give file URL directly
                    })
                # --- ADD DEFAULT LICENSE ---
                if not licenses_list:
                    logger.debug(f"No license found via API for {repo.name}. Applying default: Apache License 2.0")
                    licenses_list.append({
                        "name": "Apache License 2.0",
                        "URL": "https://www.apache.org/licenses/LICENSE-2.0"
                    })
                # --- END DEFAULT LICENSE ---

                # --- Fetch README Content ---
                readme_content_str: Optional[str] = None
                readme_url: Optional[str] = None # Initialize readme_url
                try:
                    readme_file = repo.get_readme()
                    readme_url = readme_file.html_url # Store the URL early
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
                # --- END Fetch ---


                # Build the dictionary using schema 2.0 field names where possible
                # This dictionary will be passed to the exemption processor
                repo_data = {
                    # === Core Schema Fields ===
                    "name": repo.name,
                    "description": repo.description or '',
                    "organization": repo.owner.login, # Use owner login as organization
                    "repositoryURL": repo.html_url,
                    "homepageURL": repo.html_url, # Default to repo URL, adjust if specific homepage exists
                    "downloadURL": None, # Requires specific release asset info, not available here
                    "vcs": "git",
                    "repositoryVisibility": repo_visibility,
                    "status": "development", # Placeholder - could be inferred later (e.g., based on activity)
                    "version": "N/A", # Placeholder - requires fetching tags/releases
                    "laborHours": 0, # Placeholder - requires estimation logic
                    "languages": [repo_language] if repo_language else [], # Schema expects a list
                    "tags": [], # Placeholder - requires fetching tags

                    # === Nested Schema Fields ===
                    "date": {
                        "created": created_at_iso,
                        "lastModified": pushed_at_iso, # Use pushed_at as best indicator of code change
                        # "metadataLastUpdated": Will be added globally later
                    },
                    "permissions": {
                        "usageType": None, # To be determined by exemption_processor
                        "exemptionText": None, # To be determined by exemption_processor
                        "licenses": licenses_list
                    },
                    "contact": {
                        "name": "Centers for Disease Control and Prevention", # Default contact name
                        "email": None # To be determined by exemption_processor
                    },
                    "contractNumber": None, # To be determined by exemption_processor

                    # === Fields needed for processing (will be removed later) ===
                    "readme_content": readme_content_str, # Pass fetched content
                    "_codeowners_content": codeowners_content_str, # Pass fetched content
                    "_is_private_flag": repo.private, # Temp flag for exemption_processor logic
                    "_language_heuristic": repo_language, # Temp field for exemption_processor non-code check

                    # === /Additional Fields (Kept at the end) ===
                    "repo_id": repo.id,
                    "readme_url": readme_url, # Pass the found URL
                }

                # --- Call Exemption Processor ---
                # The processor will modify repo_data directly (permissions, contact, contractNumber)
                # It needs access to 'readme_content', '_is_private_flag', '_language_heuristic' etc.
                processed_data = exemption_processor.process_repository_exemptions(repo_data) # Pass the dict

                # --- Clean up temporary/processed fields ---
                # Processor now handles removing readme_content and _codeowners_content
                processed_data.pop('_is_private_flag', None)
                processed_data.pop('_language_heuristic', None)

                # Add the fully processed data to the list
                processed_repo_list.append(processed_data)

                # --- INCREMENT GLOBAL COUNTER ---
                # Increment only after successfully processing and appending
                processed_counter[0] += 1
                # --- END INCREMENT ---


            except Exception as repo_err:
                logger.error(f"Error processing GitHub repository '{repo.name}' (within main loop): {repo_err}", exc_info=True)
                # Optionally append minimal error info if needed downstream
                # processed_repo_list.append({'repo_name': repo.name, 'error': str(repo_err)})
                # Do NOT increment counter if an error occurred during processing this repo

        logger.info(f"Finished GitHub scan. Processed {len(processed_repo_list)} repositories in this connector. Global count: {processed_counter[0]}")

    # --- Exception Handling (as before) ---
    except BadCredentialsException:
        logger.error(f"GitHub authentication failed. Check GITHUB_TOKEN. Skipping.")
        return []
    except UnknownObjectException:
        logger.error(f"GitHub organization '{org_name}' not found. Check GITHUB_ORG. Skipping.")
        return []
    except GithubException as e:
        logger.error(f"GitHub API error: {e.status} {e.data.get('message', '')}. Skipping.", exc_info=True)
        return []
    except RequestException as e:
        logger.error(f"Network error connecting to GitHub: {e}. Skipping.", exc_info=True)
        return []
    except Exception as e:
        logger.error(f"Unexpected error during GitHub fetch for org '{org_name}': {e}. Skipping.", exc_info=True)
        return []

    return processed_repo_list
