# utils/caching.py
import json
import os
import logging
from typing import Dict, Optional, List

logger = logging.getLogger(__name__)

# A dictionary to map platform names to their typical unique ID field and commit SHA field
# names *as expected in the cached JSON file*.
PLATFORM_CACHE_CONFIG = {
    "github": {"id_field": "repo_id", "commit_sha_field": "lastCommitSHA"}, 
    "gitlab": {"id_field": "repo_id", "commit_sha_field": "lastCommitSHA"}, 
    "azure": {"id_field": "repo_id", "commit_sha_field": "lastCommitSHA"}    
}

def _parse_org_from_filename(file_path: str, platform: str) -> Optional[str]:
    """
    Helper to parse organization/group slug from the intermediate filename.
    Assumes filename format like: intermediate_<platform>_<org_slug>.json
    """
    if not file_path or not platform:
        return None
    
    basename = os.path.basename(file_path)
    prefix = f"intermediate_{platform.lower()}_"
    if basename.startswith(prefix) and basename.endswith(".json"):
        org_slug = basename[len(prefix):-len(".json")]
        # Sanity check: ensure it's not an empty string or overly complex
        if org_slug and "/" not in org_slug and "\\" not in org_slug:
            return org_slug
    return None

def load_previous_scan_data(file_path: str, platform: str) -> Dict[str, Dict]:
    """
    Loads previous scan data from a JSON file and returns a dictionary
    keyed by a unique repository identifier, for repositories that have a commit SHA.
    """
    previous_data_map: Dict[str, Dict] = {}
    if not os.path.exists(file_path):
        # Try to derive org_slug for context even if file not found
        org_slug_context = _parse_org_from_filename(file_path, platform) or platform
        logger.info(f"No previous scan data file found at {file_path}. Proceeding with full scan for this target.", extra={'org_group': org_slug_context})
        return previous_data_map

    platform_key = platform.lower()
    cache_config = PLATFORM_CACHE_CONFIG.get(platform_key)
    if not cache_config:
        logger.error(f"Unsupported platform '{platform}' for caching. Cannot determine key fields. Check PLATFORM_CACHE_CONFIG.", extra={'org_group': platform})
        return previous_data_map

    id_field_in_cache = cache_config["id_field"]
    commit_sha_field_in_cache = cache_config["commit_sha_field"]
    org_slug_from_filename = _parse_org_from_filename(file_path, platform)

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            # The file is expected to be a list of repository data dictionaries (final code.json entries)
            data_list: List[Dict] = json.load(f)

        for repo_entry in data_list:
            repo_id_value = repo_entry.get(id_field_in_cache)
            repo_key_str: Optional[str] = None

            if repo_id_value is not None:
                repo_key_str = str(repo_id_value) # Ensure string key
            elif platform_key == "github" and org_slug_from_filename:
                # Fallback for GitHub: try to construct fullName from org_slug (from filename) and repo name
                repo_name_from_cache = repo_entry.get('name')
                if repo_name_from_cache:
                    repo_key_str = f"{org_slug_from_filename}/{repo_name_from_cache}" # org_slug_from_filename is the org context
                    logger.debug(f"Derived GitHub key '{repo_key_str}' from filename and repo name for entry: {str(repo_entry)[:100]}...", extra={'org_group': org_slug_from_filename})
            # Fallback for GitLab/Azure: if the canonical 'repo_id' (as per updated config) is missing, try 'id'
            # This handles caches generated when 'id' was the primary field.
            elif platform_key in ["gitlab", "azure"] and repo_id_value is None and repo_entry.get('id') is not None:
                repo_key_str = str(repo_entry.get('id'))
                logger.debug(f"Using fallback 'id' as key for {platform_key} entry (repo_id not found): {repo_key_str} from {str(repo_entry)[:100]}...", extra={'org_group': org_slug_from_filename or platform_key})
            # Further fallbacks if primary ID and 'repo_id' are missing
            elif platform_key == "gitlab": # Fallback to path_with_namespace if 'id' and 'repo_id' are missing
                    repo_key_str = repo_entry.get('path_with_namespace')
            elif platform_key == "azure": # Fallback to constructed name if 'id' and 'repo_id' are missing
                    org_name = repo_entry.get('organization', {}).get('name')
                    project_name = repo_entry.get('project', {}).get('name')
                    repo_name_val = repo_entry.get('name')
                    if org_name and project_name and repo_name_val:
                        repo_key_str = f"{org_name}/{project_name}/{repo_name_val}"
            # else: # No suitable key found after primary and fallbacks

            if repo_key_str:
                # Entry is only cacheable if it has the commit SHA field populated
                if repo_entry.get(commit_sha_field_in_cache):
                    previous_data_map[repo_key_str] = repo_entry
                else:
                    # Use repo_key_str as context if available, else org_slug_from_filename
                    log_context_for_missing_sha = repo_key_str if "/" in str(repo_key_str) else (org_slug_from_filename or platform_key)
                    logger.debug(f"Previous entry for '{repo_key_str}' (Platform: {platform}) in {file_path} missing '{commit_sha_field_in_cache}'. Will not be used for caching.", extra={'org_group': log_context_for_missing_sha})
            else:
                logger.warning(f"Could not determine a unique key for an entry in {file_path} "
                               f"(Platform: {platform}, Name: {repo_entry.get('name', 'N/A')}). "
                               f"Expected ID field: '{id_field_in_cache}'. Entry snippet: {str(repo_entry)[:100]}...", extra={'org_group': org_slug_from_filename or platform_key})

        logger.info(f"Successfully loaded {len(previous_data_map)} cacheable entries from previous scan: {file_path} for platform {platform}", extra={'org_group': org_slug_from_filename or platform_key})
    except json.JSONDecodeError as e:
        logger.error(f"JSON decoding error loading previous scan data from {file_path}: {e}", exc_info=True, extra={'org_group': org_slug_from_filename or platform_key})
    except IOError as e:
        logger.error(f"IOError loading previous scan data from {file_path}: {e}", exc_info=True, extra={'org_group': org_slug_from_filename or platform_key})
    except Exception as e: # Catch any other unexpected errors
        logger.error(f"Unexpected error loading or parsing previous scan data from {file_path}: {e}", exc_info=True, extra={'org_group': org_slug_from_filename or platform_key})

    return previous_data_map
