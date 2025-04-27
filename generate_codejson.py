# generate_codejson.py

import os
import json
import logging
import logging.handlers
import time
import re
from datetime import datetime, timezone # Ensure timezone is imported
from dotenv import load_dotenv

# Import connectors
import github_connector
import gitlab_connector
import azure_devops_connector
# Import utils
from utils import ExemptionLogger, PrivateIdManager

# --- setup_logging() function remains the same ---
def setup_logging():
    """Configures logging for the application."""
    log_directory = "logs"
    log_file = os.path.join(log_directory, "repo_scan.log")
    os.makedirs(log_directory, exist_ok=True)
    log_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger()
    if logger.hasHandlers():
        logger.handlers.clear()
    logger.setLevel(logging.INFO) # Set level (e.g., INFO, DEBUG)

    # Console Handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_format)
    logger.addHandler(console_handler)

    # Rotating File Handler
    try:
        file_handler = logging.handlers.RotatingFileHandler(
            log_file, maxBytes=5*1024*1024, backupCount=5, encoding='utf-8'
        )
        file_handler.setFormatter(log_format)
        logger.addHandler(file_handler)
        logging.info("Logging configured: Outputting to console and %s", log_file)
    except Exception as e:
        logging.error(f"Failed to configure file logging to {log_file}: {e}")
        logging.info("Logging configured: Outputting to console only.")

# --- write_output_files() function remains the same ---
def write_output_files(data, filename="code.json"):
    """Writes the provided data to a JSON file."""
    output_directory = "output"
    output_path = os.path.join(output_directory, filename)
    os.makedirs(output_directory, exist_ok=True)
    try:
        # Use 'w' mode to always create a new file or overwrite existing
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        logging.info(f"Successfully wrote output data to {output_path}")
    except TypeError as e:
        logging.error(f"Data type error writing JSON to {output_path}: {e}. Ensure all data is JSON serializable.")
    except IOError as e:
        logging.error(f"File I/O error writing JSON to {output_path}: {e}")
    except Exception as e:
        logging.error(f"Unexpected error writing JSON to {output_path}: {e}", exc_info=True)

# --- Backup Function (remains the same) ---
def backup_existing_file(output_dir="output", filename="file.ext"):
    """
    Checks for an existing file and renames it with a date/timestamp
    to prevent overwriting on subsequent runs. Handles different file types.
    """
    logger = logging.getLogger(__name__) # Get logger instance
    current_filepath = os.path.join(output_dir, filename)

    if os.path.isfile(current_filepath):
        try:
            # Generate timestamped backup filename
            now = datetime.now()
            timestamp_str = now.strftime("%Y%m%d_%H%M%S")
            base_name, ext = os.path.splitext(filename)
            # Ensure extension starts with a dot if it exists
            ext = f".{ext.lstrip('.')}" if ext else ""
            backup_filename = f"{base_name}_{timestamp_str}{ext}"
            backup_filepath = os.path.join(output_dir, backup_filename)

            # Ensure the backup name doesn't somehow already exist
            counter = 1
            while os.path.exists(backup_filepath):
                 backup_filename = f"{base_name}_{timestamp_str}_{counter}{ext}"
                 backup_filepath = os.path.join(output_dir, backup_filename)
                 counter += 1
                 logger.warning(f"Backup file collision, trying {backup_filename}")

            # Perform the rename
            os.rename(current_filepath, backup_filepath)
            logger.info(f"Existing '{filename}' found. Renamed to '{backup_filename}'.")

        except OSError as e:
            logger.error(f"Error renaming existing '{filename}': {e}", exc_info=True)
        except Exception as e:
            logger.error(f"Unexpected error during backup of '{filename}': {e}", exc_info=True)
    else:
        logger.info(f"No existing '{filename}' found to back up. A new file will be created.")

def infer_tags(repo_data):
    """Infers tags from repository topics/tag_list."""
    # 'tags' field is already populated by connectors using topics/tag_list
    topics = repo_data.get('tags', []) # Use the 'tags' field directly
    if topics and isinstance(topics, list):
        return sorted([str(topic) for topic in topics])
    return []

def parse_semver(tag_name):
    """Attempts to parse a semantic version string, handling common prefixes."""
    if not tag_name or not isinstance(tag_name, str): return None
    cleaned_tag = re.sub(r'^(v|release-|Release-|jenkins-\S+-)', '', tag_name.strip()) # More prefixes
    try: return packaging_version.parse(cleaned_tag)
    except packaging_version.InvalidVersion: return None

def infer_version(repo_data):
    """Infers the latest semantic version from API tags stored in _api_tags."""
    # Use the temporary field where connectors stored actual Git tags
    api_tags = repo_data.get('_api_tags', [])
    if not api_tags: return "N/A"

    parsed_versions = []
    for tag_name in api_tags:
        parsed = parse_semver(tag_name)
        if parsed and not parsed.is_prerelease: # Prioritize stable releases
            parsed_versions.append(parsed)

    if parsed_versions:
        parsed_versions.sort()
        return str(parsed_versions[-1]) # Latest stable version

    # Fallback: If no stable versions, check for pre-releases
    parsed_prereleases = []
    for tag_name in api_tags:
        parsed = parse_semver(tag_name)
        if parsed and parsed.is_prerelease:
             parsed_prereleases.append(parsed)
    if parsed_prereleases:
         parsed_prereleases.sort()
         return str(parsed_prereleases[-1]) # Latest pre-release

    return "N/A" # No parsable versions found

def infer_status(repo_data):
    """Infers status based on archived flag and potentially README/activity later."""
    # Use the 'archived' field passed from connectors
    if repo_data.get('archived', False):
         return "archived"
    # TODO: Add README keyword search (e.g., "Status: Maintained", "Status: Deprecated")
    # TODO: Add check based on lastModified date for inactivity
    return "development" # Default fallback


# --- Main Execution ---
if __name__ == "__main__":
    load_dotenv()
    setup_logging()
    logger = logging.getLogger(__name__)

    # Set this to True to limit processing repos and for testing or debuging
    DEBUG_LIMIT_REPOS = False
    DEBUG_REPO_LIMIT = 20
    # --- File Paths ---
    # Use environment variables.  See .env file
    OUTPUT_DIR = os.getenv("OutputDir", "output") .strip()
    CODE_JSON_FILENAME = os.getenv("catalogJsonFile", "code.json")
    EXEMPTION_LOG_FILENAME = os.getenv("ExemptedCSVFile", "exempted_log.csv") # Get just filename
    PRIVATE_ID_FILENAME = os.getenv("PrivateIDCSVFile", "privateid_mapping.csv") # Get just filename
    EXEMPTION_FILE_PATH = os.path.join(OUTPUT_DIR, EXEMPTION_LOG_FILENAME)
    PRIVATE_ID_FILE_PATH = os.path.join(OUTPUT_DIR, PRIVATE_ID_FILENAME)

    # Note: Import processor (can be done here or globally)
    # but is already imported by each connector


    # --- Read Instructions PDF URL from Environment ---
    INSTRUCTIONS_URL = os.getenv("INSTRUCTIONS_PDF_URL")
    if not INSTRUCTIONS_URL:
        logger.warning("INSTRUCTIONS_PDF_URL environment variable not set. Private repository URLs will not be replaced.")
    
    # --- Backup existing CSV files ---
    backup_existing_file(output_dir=OUTPUT_DIR, filename=CODE_JSON_FILENAME)
    backup_existing_file(output_dir=OUTPUT_DIR, filename=EXEMPTION_LOG_FILENAME)

    # --- File Paths and Manager Initialization ---
    logger.debug(f"Using EXEMPTION_FILE path: '{EXEMPTION_FILE_PATH}'")
    logger.debug(f"Using PRIVATE_ID_FILE path: '{PRIVATE_ID_FILE_PATH}'")
    
    # Manager initialization
    try:
        # Ensure output directory exists before initializing managers that write there
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        exemption_manager = ExemptionLogger(EXEMPTION_FILE_PATH)
        privateid_manager = PrivateIdManager(PRIVATE_ID_FILE_PATH)
    except Exception as mgr_err:
        logger.critical(f"Failed to initialize managers: {mgr_err}", exc_info=True)
        exit(1)

    # --- Environment Variables for Connectors ---
    GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
    GITHUB_ORG = os.getenv("GITHUB_ORG")
    GITLAB_TOKEN = os.getenv("GITLAB_TOKEN")
    GITLAB_GROUP = os.getenv("GITLAB_GROUP")
    AZURE_DEVOPS_TOKEN = os.getenv("AZURE_DEVOPS_TOKEN")
    AZURE_DEVOPS_ORG = os.getenv("AZURE_DEVOPS_ORG")
    AZURE_DEVOPS_PROJECT = os.getenv("AZURE_DEVOPS_PROJECT")

    # --- Fetch Data from Connectors ---
    all_processed_repos = []
    global_processed_count = [0]
    limit_to_pass = DEBUG_REPO_LIMIT if DEBUG_LIMIT_REPOS else None

    if DEBUG_LIMIT_REPOS:
        logger.warning(f"--- DEBUG MODE ACTIVE: Global processing limit set to {DEBUG_REPO_LIMIT} repositories ---")

    # -----------------
    # --- GitHub ---
    # -----------------
    if limit_to_pass is None or global_processed_count[0] < limit_to_pass:
        logger.info(f"Attempting GitHub scan...")
        try:
            # Pass counter and limit to the connector
            github_repos = github_connector.fetch_repositories(
                GITHUB_TOKEN,
                GITHUB_ORG,
                processed_counter=global_processed_count, # Pass the mutable list
                debug_limit=limit_to_pass
            )
            all_processed_repos.extend(github_repos)
            if github_repos: logger.info(f"Received {len(github_repos)} processed repositories from GitHub. Total processed so far: {global_processed_count[0]}")
        except Exception as e:
            logger.error(f"Critical error during GitHub fetch/process: {e}", exc_info=True)
    else:
        logger.warning("--- DEBUG MODE: Global limit reached. Skipping GitHub scan. ---")


    # -----------------
    # --- GitLab ---
    # -----------------
    if limit_to_pass is None or global_processed_count[0] < limit_to_pass:
        logger.info(f"Attempting GitLab scan...")
        try:
            # Pass counter and limit to the connector
            gitlab_repos = gitlab_connector.fetch_repositories(
                GITLAB_TOKEN,
                GITLAB_GROUP,
                processed_counter=global_processed_count, # Pass the mutable list
                debug_limit=limit_to_pass
            )
            all_processed_repos.extend(gitlab_repos)
            if gitlab_repos: logger.info(f"Received {len(gitlab_repos)} processed repositories from GitLab. Total processed so far: {global_processed_count[0]}")
        except Exception as e:
            logger.error(f"Critical error during GitLab fetch/process: {e}", exc_info=True)
    else:
        logger.warning("--- DEBUG MODE: Global limit reached. Skipping GitLab scan. ---")



    # -----------------
    # --- Azure DevOps ---
    # -----------------
    if limit_to_pass is None or global_processed_count[0] < limit_to_pass:
        logger.info(f"Attempting Azure DevOps scan...")
        try:
            # Pass counter and limit to the connector
            azure_repos = azure_devops_connector.fetch_repositories(
                AZURE_DEVOPS_TOKEN,
                AZURE_DEVOPS_ORG,
                AZURE_DEVOPS_PROJECT,
                processed_counter=global_processed_count, # Pass the mutable list
                debug_limit=limit_to_pass
            )
            all_processed_repos.extend(azure_repos)
            if azure_repos: logger.info(f"Received {len(azure_repos)} processed repositories from Azure DevOps. Total processed so far: {global_processed_count[0]}")
        except Exception as e:
            logger.error(f"Critical error during Azure DevOps fetch/process: {e}", exc_info=True)
    else:
        logger.warning("--- DEBUG MODE: Global limit reached. Skipping Azure DevOps scan. ---")
        

    # --- Final Processing Loop (ID Generation and Exemption Logging) ---
    logger.info(f"Total processed repositories received from connectors: {len(all_processed_repos)}")
    final_output_list = []
    total_received = len(all_processed_repos) # This is now the limited number

    if all_processed_repos:
        logger.info("Generating Private IDs and logging exemptions...")
        # The loop counter 'i' is fine here, no separate debug counter needed now
        for i, repo_data in enumerate(all_processed_repos):
            count = i + 1 # Original counter for logging progress
            repo_name = repo_data.get('name', f'UnknownRepo_{count}')
            org_name = repo_data.get('organization', 'UnknownOrg')
            is_private = repo_data.get('repositoryVisibility') == 'private'

            logger.debug(f"Final processing for repo {count}/{total_received}: {org_name}/{repo_name}")

           # Check for processing errors from connector stage
            if 'processing_error' in repo_data:
                 logger.error(f"Skipping final processing for {org_name}/{repo_name} due to connector error: {repo_data['processing_error']}")
                 # Add minimal error entry to output
                 final_output_list.append({
                     "name": repo_name,
                     "organization": org_name,
                     "processing_error": repo_data['processing_error']
                 })
                 continue # Skip to next repo
             
            try:
                # This field is populated by exemption_processor now
                private_emails_list = repo_data.get('_private_contact_emails', [])

                # --- Get/Generate Private ID (stores actual emails in CSV) ---
                private_id = privateid_manager.get_or_generate_id(
                    repo_name=repo_name,
                    organization=org_name,
                    contact_emails=private_emails_list # Pass actual emails
                )
                if is_private:
                    repo_data['privateID'] = private_id
                else:
                    repo_data.pop('privateID', None)

                # --- UPDATE repositoryURL for private repos ---
                if is_private and INSTRUCTIONS_URL:
                    repo_data['repositoryURL'] = INSTRUCTIONS_URL
                elif is_private: logger.warning(f"Private repo {repo_name}: INSTRUCTIONS_PDF_URL not set.")

                # --- Log Exemption ---
                usage_type = repo_data.get('permissions', {}).get('usageType')
                if usage_type and usage_type.lower().startswith('exempt'):
                    exemption_text = repo_data.get('permissions', {}).get('exemptionText', '')
                    log_id = private_id if is_private else f"PublicRepo-{org_name}-{repo_name}"
                    exemption_manager.log_exemption(log_id, repo_name, usage_type, exemption_text)

               # --- Apply Final Inference ---
                repo_data['status'] = infer_status(repo_data)
                repo_data['version'] = infer_version(repo_data)
                # 'tags' field is already populated by connectors using topics/tag_list

                # --- Ensure Datetime Conversion ---
                if 'date' in repo_data and isinstance(repo_data['date'], dict):
                    for key, value in repo_data['date'].items():
                         if isinstance(value, datetime): repo_data['date'][key] = value.isoformat()

                # --- Remove temporary fields used only for processing/inference ---
                repo_data.pop('_private_contact_emails', None)
                repo_data.pop('_api_tags', None) # Remove the temp field for Git tags
                repo_data.pop('archived', None) # Remove the temp field for archived status

                # --- Clean up None values before adding to list ---
                # Create a new dict to avoid modifying the original while iterating
                cleaned_repo_data = {}
                for k, v in repo_data.items():
                    if isinstance(v, dict):
                         # Clean nested dictionaries (date, permissions, contact)
                         cleaned_v = {nk: nv for nk, nv in v.items() if nv is not None}
                         if cleaned_v: # Only add if not empty after cleaning
                              cleaned_repo_data[k] = cleaned_v
                    elif v is not None:
                         cleaned_repo_data[k] = v

                # Ensure essential nested dicts exist even if empty after cleaning (optional, depends on schema strictness)
                # if 'date' not in cleaned_repo_data: cleaned_repo_data['date'] = {}
                # if 'permissions' not in cleaned_repo_data: cleaned_repo_data['permissions'] = {}
                # if 'contact' not in cleaned_repo_data: cleaned_repo_data['contact'] = {}

                final_output_list.append(repo_data)

            except Exception as final_proc_err:
                logger.error(f"Error during final processing (ID gen/logging) for {org_name}/{repo_name}: {final_proc_err}", exc_info=True)
                final_output_list.append({
                    "name": repo_name,
                    "organization": org_name,
                    "processing_error": f"Final stage: {final_proc_err}"
                })


        logger.info(f"Finished final processing loop for {len(final_output_list)} repositories.")
    else:
        logger.info("No processed repositories received from connectors.")

    # --- Prepare final JSON structure ---
    final_code_json_structure = {
        "version": "2.0",
        "agency": "CDC",
        "measurementType": { "method": "projects" },
        "projects": final_output_list
    }

    # --- Add metadataLastUpdated timestamp ---
    now_iso = datetime.now(timezone.utc).isoformat()
    for project in final_code_json_structure["projects"]:
        if "date" not in project or not isinstance(project.get("date"), dict):
            if "processing_error" in project: continue
            project["date"] = {}
        project["date"]["metadataLastUpdated"] = now_iso

     # --- Write final output (CODE.JSON) ---
    logger.info(f"Writing final data for {len(final_output_list)} repositories to {CODE_JSON_FILENAME}...")
    write_output_files(final_code_json_structure, filename=CODE_JSON_FILENAME)

    # --- Save Private IDs to CSV ---
    try:
        # Change save_new_mappings to save_all_mappings
        privateid_manager.save_all_mappings()
    except Exception as save_err:
        logger.error(f"Error occurred during final save of private IDs: {save_err}", exc_info=True)

    # --- Log Summary ---
    logging.info("--- Run Summary ---")
    # Use the global counter for total received before limit
    logging.info(f"Total repositories processed by connectors (before limit): {global_processed_count[0]}")
    processed_in_final_loop = len(final_output_list) # Count items actually in the final list
    logging.info(f"Total repositories processed in final loop: {processed_in_final_loop}")
    logging.info(f"Total repositories in final output: {len(final_output_list)}")
    new_ids = privateid_manager.get_new_id_count() # Count IDs generated *since last save*
    new_exemptions = exemption_manager.get_new_exemption_count()
    logging.info(f"New private IDs generated (this run): {new_ids}")
    logging.info(f"New exemptions logged (this run): {new_exemptions}")
    logging.info("-------------------")

    logging.info("Pausing briefly before exit...")
    time.sleep(3)
    logging.info("Script finished.")


