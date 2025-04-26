# github_connector.py
import os
import logging
from datetime import datetime
from github import Github, BadCredentialsException, UnknownObjectException, GithubException
from requests.exceptions import RequestException
import base64

logger = logging.getLogger(__name__)

def is_placeholder_token(token):
    """Checks if the token is missing or likely a placeholder."""
    return not token or (token.startswith("ghp_") and len(token) < 40)

def fetch_repositories(token, org_name) -> list[dict]:
    """
    Fetches repository details from GitHub, processes exemptions,
    and returns a list of processed repository data dictionaries.
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

        count = 0
        for i, repo in enumerate(repos):
            count = i + 1
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
                created_at_iso = repo.created_at.isoformat() if repo.created_at else None
                updated_at_iso = repo.updated_at.isoformat() if repo.updated_at else None
                pushed_at_iso = repo.pushed_at.isoformat() if repo.pushed_at else None

                repo_data = {
                    'source': 'GitHub',
                    'id': repo.id,
                    'repo_name': repo.name,
                    'full_name': repo.full_name,
                    'description': repo.description or '',
                    'url': repo.html_url,
                    'html_url': repo.html_url,
                    'api_url': repo.url,
                    'is_private': repo.private,
                    'org_name': repo.owner.login, # Initial org name
                    'created_at': created_at_iso,
                    'updated_at': updated_at_iso,
                    'pushed_at': pushed_at_iso,
                    'last_updated': pushed_at_iso,
                    'languages_url': repo.languages_url,
                    'tags_url': repo.tags_url,
                    'contents_url': repo.contents_url.replace('{+path}', ''),
                    'commits_url': repo.commits_url.replace('{/sha}', ''),
                    'license': {'name': repo.license.name, 'key': repo.license.key} if repo.license else None,
                    "default_branch": repo.default_branch,
                    "language": repo.language, # Primary language
                    "readme_url": None,
                    "readme_content": None, # Will be fetched next
                    "contact_email": None,
                    # Exemption fields will be added by the processor
                }

                # --- Fetch README Content ---
                try:
                    readme_file = repo.get_readme()
                    readme_content_bytes = base64.b64decode(readme_file.content)
                    try:
                        repo_data['readme_content'] = readme_content_bytes.decode('utf-8')
                    except UnicodeDecodeError:
                        try:
                            repo_data['readme_content'] = readme_content_bytes.decode('latin-1')
                            logger.warning(f"Decoded README for {repo.name} using latin-1 encoding.")
                        except Exception:
                            repo_data['readme_content'] = readme_content_bytes.decode('utf-8', errors='ignore')
                            logger.warning(f"Decoded README for {repo.name} using utf-8, ignoring errors.")
                    repo_data['readme_url'] = readme_file.html_url
                    logger.debug(f"Successfully fetched README for {repo.name}")
                except UnknownObjectException:
                    logger.debug(f"No README found for repository: {repo.name}")
                    repo_data['readme_content'] = None # Ensure it's None if not found
                except Exception as readme_err:
                    logger.error(f"Error fetching or decoding README for {repo.name}: {readme_err}", exc_info=True)
                    repo_data['readme_content'] = None # Ensure it's None on error

                # --- Call Exemption Processor ---
                # Pass the collected repo_data to the central processor
                processed_data = exemption_processor.process_repository_exemptions(repo_data)

                # --- Clean up before adding to list ---
                # Remove readme_content as it's large and processed now
                processed_data.pop('readme_content', None)

                # Add the fully processed data to the list
                processed_repo_list.append(processed_data)

            except Exception as repo_err:
                logger.error(f"Error processing GitHub repository '{repo.name}' (within main loop): {repo_err}", exc_info=True)
                # Optionally append minimal error info if needed downstream
                # processed_repo_list.append({'repo_name': repo.name, 'error': str(repo_err)})

        logger.info(f"Successfully fetched and processed {len(processed_repo_list)} total repositories from GitHub organization '{org_name}'.")

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

# --- Note: Apply similar changes to gitlab_connector.py and azure_devops_connector.py ---
# 1. Import exemption_processor
# 2. Fetch base data + README + language into a repo_data dict
# 3. Call exemption_processor.process_repository_exemptions(repo_data)
# 4. Pop 'readme_content' from the result
# 5. Append the result to the list returned by the connector
