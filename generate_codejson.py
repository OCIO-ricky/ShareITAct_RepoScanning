# generate_codejson.py

import os
import json
import logging
import logging.handlers
import time
from datetime import datetime # Ensure datetime is imported
from dotenv import load_dotenv
import github_connector
import gitlab_connector
import azure_devops_connector
# Import the classes from utils
from utils import ExemptionLogger, PrivateIdManager

# --- Define setup_logging ---
def setup_logging():
    """Configures logging for the application."""
    log_directory = "logs"
    log_file = os.path.join(log_directory, "repo_scan.log")

    # Create logs directory if it doesn't exist
    os.makedirs(log_directory, exist_ok=True)

    # Define log format
    log_format = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Get root logger
    logger = logging.getLogger()
    # Clear existing handlers (useful if script is run multiple times in one session)
    if logger.hasHandlers():
        logger.handlers.clear()

    # Set the default level back to INFO
    logger.setLevel(logging.INFO)

    # --- Console Handler ---
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_format)
    logger.addHandler(console_handler)

    # --- Rotating File Handler ---
    # Rotates logs, keeping 5 backups, max 5MB each
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


# --- Define write_output_files ---
def write_output_files(data, filename="code.json"):
    """Writes the provided data to a JSON file."""
    output_directory = "output"
    output_path = os.path.join(output_directory, filename)

    # Create output directory if it doesn't exist
    os.makedirs(output_directory, exist_ok=True)

    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            # Use indent for pretty-printing the JSON file
            json.dump(data, f, ensure_ascii=False, indent=4)
        logging.info(f"Successfully wrote output data to {output_path}")
    except TypeError as e:
        logging.error(f"Data type error writing JSON to {output_path}: {e}. Ensure all data is JSON serializable (check datetime objects).")
    except IOError as e:
        logging.error(f"File I/O error writing JSON to {output_path}: {e}")
    except Exception as e:
        logging.error(f"Unexpected error writing JSON to {output_path}: {e}", exc_info=True)


# --- Main Execution ---
if __name__ == "__main__":
    # --- Load environment variables ---
    load_dotenv()

    # --- Call setup_logging ---
    setup_logging() # Configure logging first

    # --- Get a logger instance for this module ---
    logger = logging.getLogger(__name__) # Or logging.getLogger() for root logger

    # --- Get file paths from environment or use defaults ---
    EXEMPTION_FILE = os.getenv("ExemptedCSVFile", "output/exempted_log.csv")
    PRIVATE_ID_FILE = os.getenv("PrivateIDCSVFile", "output/privateid_mapping.csv")
    # --- Add Debug Logging ---
    logging.debug(f"Read EXEMPTION_FILE path: '{EXEMPTION_FILE}'")
    logging.debug(f"Read PRIVATE_ID_FILE path: '{PRIVATE_ID_FILE}'")
    # --- End Debug Logging ---

    # --- Instantiate the managers ---
    # These will load existing data or create files from templates
    try:
        exemption_manager = ExemptionLogger(EXEMPTION_FILE)
        privateid_manager = PrivateIdManager(PRIVATE_ID_FILE)
    except Exception as mgr_err:
        logging.critical(f"Failed to initialize managers: {mgr_err}", exc_info=True)
        # Consider exiting if managers are critical
        exit(1) # Or handle more gracefully

    # --- Get tokens and org/group names from environment variables ---
    GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
    GITHUB_ORG = os.getenv("GITHUB_ORG")
    GITLAB_TOKEN = os.getenv("GITLAB_TOKEN")
    GITLAB_GROUP = os.getenv("GITLAB_GROUP")
    AZURE_DEVOPS_TOKEN = os.getenv("AZURE_DEVOPS_TOKEN")
    AZURE_DEVOPS_ORG = os.getenv("AZURE_DEVOPS_ORG")
    AZURE_DEVOPS_PROJECT = os.getenv("AZURE_DEVOPS_PROJECT") # Optional

    all_repos = [] # Initialize list to hold data from all platforms

    # --- GitHub ---
    # Connector now handles placeholder checks internally
    logging.info(f"Attempting GitHub scan...")
    try:
        # Call the refactored connector which handles init inside
        github_repos = github_connector.fetch_repositories(GITHUB_TOKEN, GITHUB_ORG)

        # Assuming the connector returns data in the desired common format
        # If not, adaptation/processing is needed here
        # Ensure datetime objects are converted to strings if returned by connector
        processed_github_repos = []
        for repo_data_raw in github_repos:
            # Example adaptation (adjust based on actual connector output)
            repo_data_processed = repo_data_raw.copy() # Start with fetched data
            for key, value in repo_data_processed.items():
                 # Check if value is a datetime object before calling isoformat()
                 if isinstance(value, datetime): # Example: Convert datetime to ISO string
                     repo_data_processed[key] = value.isoformat()
            processed_github_repos.append(repo_data_processed)

        all_repos.extend(processed_github_repos)
        if processed_github_repos: # Log success only if data was actually added
            logging.info(f"Successfully processed {len(processed_github_repos)} repositories from GitHub.")
        # No explicit 'else' needed if connector logged the skip/error reason
    except Exception as e:
        # Catch unexpected errors *outside* the connector function itself
        logging.error(f"Critical error during GitHub processing in main script: {e}", exc_info=True)


    # --- GitLab ---
    # The gitlab_connector.fetch_repositories handles placeholder checks internally
    logging.info(f"Attempting GitLab scan...")
    try:
        gitlab_repos = gitlab_connector.fetch_repositories(GITLAB_TOKEN, GITLAB_GROUP)
        # Assuming gitlab_connector returns data in the common format
        all_repos.extend(gitlab_repos)
        if gitlab_repos: # Log success only if data was actually added
             logging.info(f"Successfully processed {len(gitlab_repos)} repositories from GitLab.")
        # No explicit 'else' needed if connector logged the skip/error reason
    except Exception as e:
        # Catch unexpected errors *outside* the connector function itself
        logging.error(f"Critical error during GitLab processing in main script: {e}", exc_info=True)


    # --- Azure DevOps ---
    # The azure_devops_connector.fetch_repositories handles placeholder checks internally
    logging.info(f"Attempting Azure DevOps scan...")
    try:
        azure_repos = azure_devops_connector.fetch_repositories(
            AZURE_DEVOPS_TOKEN, AZURE_DEVOPS_ORG, AZURE_DEVOPS_PROJECT
        )
        # Assuming azure_devops_connector returns data in the common format
        all_repos.extend(azure_repos)
        if azure_repos: # Log success only if data was actually added
             logging.info(f"Successfully processed {len(azure_repos)} repositories from Azure DevOps.")
        # No explicit 'else' needed if connector logged the skip/error reason
    except Exception as e:
        # Catch unexpected errors *outside* the connector function itself
        logging.error(f"Critical error during Azure DevOps processing in main script: {e}", exc_info=True)


    # --- Process all_repos (Example: Add Private IDs) ---
    logging.info(f"Total repositories fetched across platforms: {len(all_repos)}")
    processed_repos_final = []
    total_fetched = len(all_repos)
    # log_interval = 100 # Log progress every 100 repos processed - Using print approach now

    if all_repos: # Only process if we actually got some repo data
        logging.info("Processing fetched repositories (adding private IDs, checking exemptions, etc.)...")
        # Use enumerate for progress counting
        for i, repo_data in enumerate(all_repos):
            count = i + 1
            try:
 
                # Ensure necessary keys exist before accessing, provide defaults
                repo_name = repo_data.get('repo_name', 'UnknownRepo')
                org_name = repo_data.get('org_name', 'UnknownOrg')

                # Get or generate private ID using the manager
                # Note: The DEBUG logs from get_or_generate_id will still print on new lines
                private_id = privateid_manager.get_or_generate_id(
                    repo_name=repo_name,
                    organization=org_name
                    # Optionally pass contact emails if available in repo_data
                )
                repo_data['privateID'] = private_id # Add the ID to the dictionary

                # --- Add other processing steps here ---
                # Example: Check exemption status based on repo_data and log if needed
                # if should_be_exempt(repo_data): # Your logic here
                #     logged_ok = exemption_manager.log_exemption(
                #         private_id=private_id,
                #         repo_name=repo_name,
                #         reason="Example Reason",
                #         usage_type="Example Usage",
                #         exemption_text="Example Text"
                #     )
                #     if logged_ok:
                #         repo_data['exempted'] = True
                #         repo_data['exemption_reason'] = "Example Reason"
                # --- End other processing steps ---

                processed_repos_final.append(repo_data)
            except Exception as proc_err:
                # Log errors using the logger (will print on a new line)
                # Print a newline first to avoid overwriting the error message
                print() # Move to next line before logging error
                logger.error(f"Error processing repository data for {repo_data.get('repo_name')}: {proc_err}", exc_info=True)


        # Log completion of processing using the logger
        logger.info(f"Finished processing {len(processed_repos_final)} repositories.")
    else:
        logging.info("No repositories fetched, skipping final processing.")


    # --- Write final output ---
    logging.info(f"Writing final data for {len(processed_repos_final)} repositories to code.json...")
    write_output_files(processed_repos_final, filename="code.json") # Call the output function

    # --- Save any new Private IDs generated during the run ---
    # (Ensure this call is present if using the save_new_mappings approach)
    try:
        privateid_manager.save_new_mappings()
    except Exception as save_err:
        logging.error(f"Error occurred during final save of private IDs: {save_err}", exc_info=True)
    # --- End save call ---

    # --- Log Summary ---
    logging.info("--- Run Summary ---")
    logging.info(f"Total repositories fetched: {total_fetched}")
    logging.info(f"Total repositories processed: {len(processed_repos_final)}")
    # Get counts from managers
    new_ids = privateid_manager.get_new_id_count()
    new_exemptions = exemption_manager.get_new_exemption_count()
    logging.info(f"New private IDs generated: {new_ids}")
    logging.info(f"New exemptions logged: {new_exemptions}")
    logging.info("-------------------")

    logging.info("Pausing briefly before exit...")
    time.sleep(3) # Pause for 3 seconds

    logging.info("Script finished.")
