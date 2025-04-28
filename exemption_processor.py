## exemption_processor.py

import re
import logging
import os
from dotenv import load_dotenv
import time
from typing import List, Optional, Dict, Any # Added typing

# --- Try importing the AI library ---
try:
    import google.generativeai as genai
    AI_ENABLED = True
except ImportError:
    AI_ENABLED = False
    genai = None

logger = logging.getLogger(__name__)

# --- Log the initial AI_ENABLED status ---
logger.info(f"Initial AI processing status (based on library import): {AI_ENABLED}")

# --- Define Exemption Codes as Constants ---
EXEMPT_BY_LAW = "exemptByLaw"
EXEMPT_NON_CODE = "exemptNonCode"
EXEMPT_BY_NATIONAL_SECURITY = "exemptByNationalSecurity"
EXEMPT_BY_AGENCY_SYSTEM = "exemptByAgencySystem"
EXEMPT_BY_MISSION_SYSTEM = "exemptByMissionSystem"
EXEMPT_BY_CIO = "exemptByCIO"
# --- Define Default Usage Types ---
USAGE_OPEN_SOURCE = "openSource"
USAGE_GOVERNMENT_WIDE_REUSE = "governmentWideReuse"
# --- End Constants ---

# List of valid exemption codes the AI can return
VALID_AI_EXEMPTION_CODES = [
    EXEMPT_BY_LAW,
    EXEMPT_BY_NATIONAL_SECURITY,
    EXEMPT_BY_AGENCY_SYSTEM,
    EXEMPT_BY_MISSION_SYSTEM,
    EXEMPT_BY_CIO,
]

# Define sensitive keywords and non-code languages centrally
SENSITIVE_KEYWORDS = ["HIPAA", "PHI", "CUI","PII","Internal use only", "Patient data"]
NON_CODE_LANGUAGES = [None, '', 'Markdown', 'Text', 'HTML', 'CSS', 'Jupyter Notebook']

# --- Load Environment Variables ---
load_dotenv() # Ensure .env is loaded
# Load MAX_TOKENS from environment (used as MAX_TOKENS in the code)
AI_MAX_TOKENS = int(os.getenv("MAX_TOKENS", 15000))
logger.info(f"Using MAX_TOKENS value: {AI_MAX_TOKENS}")

# Load AI_DELAY_ENABLED from environment
AI_DELAY_ENABLED = float(os.getenv("AI_DELAY_ENABLED", 4.5))
logger.info(f"Using AI_DELAY_ENABLED value: {AI_DELAY_ENABLED}")

# --- Get Default Emails from Environment Variables ---
# Use the exact names from the .env file provided
PRIVATE_CONTACT_EMAIL_DEFAULT = os.getenv("PRIVATE_REPO_CONTACT_EMAIL", "shareit@cdc.gov")
PUBLIC_CONTACT_EMAIL_DEFAULT = os.getenv("DEFAULT_CONTACT_EMAIL", "shareit@cdc.gov") # Changed variable name and default
logger.info(f"Using Private Repo Contact Email: {PRIVATE_CONTACT_EMAIL_DEFAULT}")
logger.info(f"Using Default Public Contact Email: {PUBLIC_CONTACT_EMAIL_DEFAULT}") # Log the correct variable
# --- End Get Default Emails ---


# --- AI Configuration ---
if AI_ENABLED:
    # load_dotenv() # Already loaded above
    GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
    if GOOGLE_API_KEY and GOOGLE_API_KEY != "YOUR_GOOGLE_API_KEY":
        try:
            genai.configure(api_key=GOOGLE_API_KEY)
            logger.info("Google Generative AI configured successfully.")
            logger.info(f"AI processing status after configuration: {AI_ENABLED}") # Log status after config
        except Exception as ai_config_err:
            logger.error(f"Failed to configure Google Generative AI: {ai_config_err}", exc_info=True)
            AI_ENABLED = False
            logger.warning(f"AI processing status after configuration failure: {AI_ENABLED}") # Log status after failure
    else:
        logger.warning("GOOGLE_API_KEY environment variable not found. AI processing will be disabled.")
        AI_ENABLED = False
        logger.info(f"AI processing status (API key missing): {AI_ENABLED}") # Log status if key missing


def _call_ai_for_organization(repo_data: dict) -> str | None:
    """Calls the AI model to suggest an organization based on repo details."""
    if not AI_ENABLED or not genai:
        logger.debug("AI processing is disabled. Skipping AI call.")
        return None

    # Use schema-aligned fields
    readme = repo_data.get('readme_content', '') or ''
    description = repo_data.get('description', '') or ''
    repo_name = repo_data.get('name', '') or ''
    xtags = repo_data.get('tags', '') or ''

    if not readme.strip() and not description.strip():
        logger.debug(f"No significant text content (README/description) found for AI analysis of '{repo_name}'. Skipping AI call.")
        return None

    max_input_length = AI_MAX_TOKENS # number of max tokens for the AI model
    input_text = f"Repository Name: {repo_name}\nDescription: {description}\nTAGS:{xtags}\n\nREADME:\n{readme}"
    if len(input_text) > max_input_length:
        input_text = input_text[:max_input_length] + "\n... [Content Truncated]"
        logger.warning(f"Input text for AI analysis of '{repo_name}' was truncated.")

    prompt = f"""
Analyze the following repository information (name, description, tags, README content)
to determine the organizations name that owns the repository. If NO specific organizational name or acronym can be infered based on the text, output ONLY the word "None".
Here is a list of known CDC organizational acronyms and names as reference. if you find text that matches perhaps an organizational acronym, output the 
corresponding organization's name. For example, the repo name "csels-datahub" would suggest the organization name is Center for Surveillance, Epidemiology, and Laboratory Services.
If you find multiple potential matches with acronyms or organizations names, use your best guess and/or output the most prominently shown across and within CDC.
cdc=Centers for Disease Control and Prevention
od=Office of the Director
os=Office of Science
olss=Office of Laboratory Science and Safety
ophdst=Office of Public Health Data, Surveillance, and Technology
orr=Office of Readiness and Response
oc=Office of Communication
oppe=Office of Policy, Performance, and Evaluation
oeeo=Office of Equal Employment Opportunity
ocio=Office of the Chief Information Officer
ocoo=Office of the Chief Operating Officer
ofr=Office of Financial Resources
osys=Office of Security
ohr=Office of Human Resources
ossam=Office of Safety, Security, and Asset Management
ogc=Office of General Counsel (CDC Legal Support)
cfa=Center for Forecasting and Outbreak Analytics
csels=Center for Surveillance, Epidemiology, and Laboratory Services
cgh=Center for Global Health
ncezid=National Center for Emerging and Zoonotic Infectious Diseases
ncird=National Center for Immunization and Respiratory Diseases
nccdphp=National Center for Chronic Disease Prevention and Health Promotion
ncipc=National Center for Injury Prevention and Control
nceh=National Center for Environmental Health
niosh=National Institute for Occupational Safety and Health
nchs=National Center for Health Statistics

    Repository Information:
    ---
    {input_text}
    ---

    Analysis Result:
    """
    try:
        logger.debug(f"Calling AI model to infer code owning organization name or acronym for repository '{repo_name}'...")
        # Ensure the model name is current and available
        model = genai.GenerativeModel('gemini-1.5-flash-latest')
        response = model.generate_content(
            prompt,
            generation_config=genai.types.GenerationConfig(temperature=0.1),
            # Consider adding safety_settings if needed
            # safety_settings=[...]
        )
        ai_result_text = response.text.strip()
        logger.debug(f"AI raw response for '{repo_name}': {ai_result_text}")

        if ai_result_text.lower() == "none":
            logger.info(f"AI analysis for '{repo_name}' determined no specific organization name was inferred.")
            return None

        organization = ai_result_text.strip()
        if organization:
            logger.info(f"AI analysis for '{repo_name}' suggests an organization: {organization}")
            return organization
        else:
            logger.warning(f"AI analysis for '{repo_name}' could not find the organization name. Ignoring.")
            return None

    except Exception as ai_err:
        # Log specific AI errors if possible (e.g., API key issues, rate limits)
        logger.error(f"Error during AI call for repository '{repo_name}': {ai_err}", exc_info=True)
        return None, None
    finally:
        # Consider making delay configurable or using exponential backoff for errors
        if AI_DELAY_ENABLED:
            delay_seconds = AI_DELAY_ENABLED 
            logger.debug(f"Pausing for {delay_seconds} seconds to respect AI rate limit...")
            time.sleep(delay_seconds)   


# --- Helper Function for AI Call ---
def _call_ai_for_exemption(repo_data: dict) -> tuple[str | None, str | None]:
    """Calls the AI model to suggest an exemption based on repo details."""
    if not AI_ENABLED or not genai:
        logger.debug("AI processing is disabled. Skipping AI call.")
        return None, None

    # Use schema-aligned fields
    readme = repo_data.get('readme_content', '') or ''
    description = repo_data.get('description', '') or ''
    repo_name = repo_data.get('name', '') # Use 'name' field

    if not readme.strip() and not description.strip():
        logger.debug(f"No significant text content (README/description) found for AI analysis of '{repo_name}'. Skipping AI call.")
        return None, None

    max_input_length =  AI_MAX_TOKENS # number of max tokens for the AI model
    input_text = f"Repository Name: {repo_name}\nDescription: {description}\n\nREADME:\n{readme}"
    if len(input_text) > max_input_length:
        input_text = input_text[:max_input_length] + "\n... [Content Truncated]"
        logger.warning(f"Input text for AI analysis of '{repo_name}' was truncated.")

    prompt = f"""
    Analyze the following repository information (name, description, README content)
    to determine if it requires a specific usage exemption based on its function
    or the data it might handle.

    Consider these specific exemption categories:
    - {EXEMPT_BY_LAW}: Data explicitly protected by law (e.g., HIPAA, FOIA mentioned).
    - {EXEMPT_BY_NATIONAL_SECURITY}: Potential national security exposure.
    - {EXEMPT_BY_AGENCY_SYSTEM}: Internal-only CDC systems (non-public facing infrastructure).
    - {EXEMPT_BY_MISSION_SYSTEM}: Logic critical to a public health mission (e.g., clinical decision support, outbreak analysis tools).
    - {EXEMPT_BY_CIO}: Requires CIO review (use this sparingly if unsure but seems sensitive/complex).

    If NO specific exemption applies based on the text, output ONLY the word "None".

    If an exemption DOES apply, output the exemption code, followed by a pipe symbol (|),
    and then a brief justification (max 2 sentences) based *only* on the provided text.

    Example Output (No Exemption):
    None

    Example Output (Exemption Found):
    {EXEMPT_BY_MISSION_SYSTEM}|The repository appears to contain logic for analyzing disease outbreak patterns based on the README description.

    Repository Information:
    ---
    {input_text}
    ---

    Analysis Result:
    """
    try:
        logger.debug(f"Calling AI model for repository '{repo_name}'...")
        # Ensure the model name is current and available
        model = genai.GenerativeModel('gemini-1.5-flash-latest')
        response = model.generate_content(
            prompt,
            generation_config=genai.types.GenerationConfig(temperature=0.1),
            # Consider adding safety_settings if needed
            # safety_settings=[...]
        )
        ai_result_text = response.text.strip()
        logger.debug(f"AI raw response for '{repo_name}': {ai_result_text}")

        if ai_result_text.lower() == "none":
            logger.info(f"AI analysis for '{repo_name}' determined no specific exemption applies.")
            return None, None

        if '|' in ai_result_text:
            parts = ai_result_text.split('|', 1)
            potential_code = parts[0].strip()
            justification = parts[1].strip()

            if potential_code in VALID_AI_EXEMPTION_CODES:
                logger.info(f"AI analysis for '{repo_name}' suggests exemption: {potential_code}. Justification: {justification}")
                return potential_code, f"AI Suggestion: {justification}"
            else:
                logger.warning(f"AI analysis for '{repo_name}' returned an invalid exemption code: '{potential_code}'. Ignoring.")
                return None, None
        else:
            logger.warning(f"AI analysis for '{repo_name}' returned an unexpected format: '{ai_result_text}'. Ignoring.")
            return None, None

    except Exception as ai_err:
        # Log specific AI errors if possible (e.g., API key issues, rate limits)
        logger.error(f"Error during AI call for repository '{repo_name}': {ai_err}", exc_info=True)
        return None, None
    finally:
        # Consider making delay configurable or using exponential backoff for errors
        delay_seconds = 4.5 # Reduced delay slightly
        logger.debug(f"Pausing for {delay_seconds} seconds to respect AI rate limit...")
        time.sleep(delay_seconds)

# --- Add Helper Regex Patterns ---
VERSION_REGEX = re.compile(r"^(?:Version|Current Version):\s*([a-zA-Z0-9v.-]{3,})", re.MULTILINE | re.IGNORECASE)
KEYWORDS_REGEX = re.compile(r"^(?:Keywords|Tags|Topics):\s*(.*)", re.MULTILINE | re.IGNORECASE)

# Looks for lines like "Status: Maintained", "Project Status: Deprecated", etc.
# Captures the status word (e.g., Maintained, Deprecated, Experimental, Active, Inactive)
STATUS_REGEX = re.compile(r"^(?:Project Status|Status):\s*(Maintained|Deprecated|Experimental|Active|Inactive)\b", re.MULTILINE | re.IGNORECASE)


# --- Add Email Extraction Helpers ---
EMAIL_PATTERN = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
EMAIL_FILTER_DOMAINS = ('.example.com', '.example.org') # Add more if needed

def _extract_emails_from_content(content: Optional[str], source_name: str) -> List[str]:
    """Generic email extraction from string content."""
    if not content:
        return []
    emails = re.findall(EMAIL_PATTERN, content)
    valid_emails = [email for email in emails if not email.lower().endswith(EMAIL_FILTER_DOMAINS)]
    logger.debug(f"Extracted emails from {source_name}: {valid_emails}")
    return valid_emails

def _get_combined_contact_emails(repo_data: Dict[str, Any]) -> List[str]:
    """Extracts emails from README and CODEOWNERS content within repo_data."""
    readme_content = repo_data.get('readme_content')
    # --- Assume CODEOWNERS content is passed in repo_data ---
    codeowners_content = repo_data.get('_codeowners_content') # Use a temporary key

    readme_emails = _extract_emails_from_content(readme_content, "README")
    codeowners_emails = _extract_emails_from_content(codeowners_content, "CODEOWNERS")

    all_emails = readme_emails + codeowners_emails
    unique_sorted_emails = sorted(list(set(email.lower() for email in all_emails)))
    logger.info(f"Combined unique contact emails for {repo_data.get('name', 'N/A')}: {unique_sorted_emails}")
    return unique_sorted_emails
# --- End Email Extraction Helpers ---

def _parse_readme_for_version(readme_content: str | None) -> str | None:
    """Attempts to extract a version string from README."""
    if not readme_content:
        return None
    match = VERSION_REGEX.search(readme_content)
    if match:
        version_str = match.group(1).strip()
        logger.debug(f"Found potential version in README via regex: '{version_str}'")
        # Basic validation (e.g., avoid excessively long strings)
        if 3 <= len(version_str) <= 30:
             # Remove leading 'v' if present for consistency, although schema allows it
             return version_str.lstrip('v')
        else:
             logger.warning(f"Ignoring potential README version '{version_str}' due to unlikely length.")
    # Add more sophisticated regex/badge parsing here if desired
    return None

def _parse_readme_for_tags(readme_content: str | None) -> list[str]:
    """Attempts to extract keywords/tags from README."""
    tags = []
    if not readme_content:
        return tags
    match = KEYWORDS_REGEX.search(readme_content)
    if match:
        keywords_line = match.group(1).strip()
        # Split by comma or space, remove empty strings
        tags = [tag.strip() for tag in re.split(r'[,\s]+', keywords_line) if tag.strip()]
        logger.debug(f"Found potential tags/keywords in README via regex: {tags}")
    # Add badge parsing here if desired
    return tags

def _parse_readme_for_status(readme_content: str | None) -> str | None:
    """Attempts to extract a status string (maintained, deprecated, etc.) from README."""
    if not readme_content:
        return None
    match = STATUS_REGEX.search(readme_content)
    if match:
        status_str = match.group(1).strip().lower() # Normalize to lowercase
        logger.debug(f"Found potential status in README via regex: '{status_str}'")
        # Map 'active' to 'maintained' for consistency if desired, or keep as 'active'
        if status_str == 'active':
            return 'maintained'
        return status_str # Return lowercase status (e.g., 'maintained', 'deprecated')
    return None

def process_repository_exemptions(repo_data: dict) -> dict:
    """
    Applies exemption logic cascade, private repo processing, and README fallbacks.
    Updates repo_data with schema fields. Extracts contact emails.

    Args:
        repo_data: Dictionary containing repository data (partially schema-aligned).
                   Expected temporary fields: 'readme_content', '_is_private_flag',
                   '_language_heuristic', '_codeowners_content'.

    Returns:
        The modified repo_data dictionary.
    """
    # --- Get data using new field names ---
    readme_content = repo_data.get('readme_content')
    language = repo_data.get('_language_heuristic') # Use temp field
    is_private = repo_data.get('_is_private_flag', False) # Use temp field
    repo_name = repo_data.get('name', 'UnknownRepo') # Use schema field 'name'
    org_name = repo_data.get('organization', 'UnknownOrg') # Use schema field 'organization'

    logger.debug(f"Processing exemptions/fallbacks for: {org_name}/{repo_name}")

    # --- Ensure nested structures exist ---
    if 'permissions' not in repo_data:
        repo_data['permissions'] = {}
    if 'contact' not in repo_data:
        repo_data['contact'] = {}

    # --- Initialize/Clear relevant output fields ---
    repo_data['permissions']['usageType'] = None
    repo_data['permissions']['exemptionText'] = None
    repo_data['contractNumber'] = repo_data.get('contractNumber')
    repo_data['contact']['email'] = repo_data['contact'].get('email') # Keep if connector set a default
    # repo_data['contact']['all_extracted_emails'] = [] # This is internal now, removed later
    repo_data['_private_contact_emails'] = [] # For passing to PrivateIdManager

    # --- Combine Email Extraction ---
    # This extracts emails from README and CODEOWNERS content passed in repo_data
    actual_contact_emails = _get_combined_contact_emails(repo_data)
    # Store actual emails temporarily for PrivateIdManager
    repo_data['_private_contact_emails'] = actual_contact_emails
    # Store all found emails temporarily for deciding the public email
    all_extracted_emails_temp = actual_contact_emails # Use a temp variable

     # Flag to track if an exemption was applied
    exemption_applied = False

    # --- Private Repository Processing (Organization name, Contract) ---
    ##if is_private and readme_content:
    if readme_content:
        logger.debug(f"Applying repo README parsing for Org/Contract: {org_name}/{repo_name}")
        # Org processing
        org_match = re.search(r"^Org:\s*(.*)", readme_content, re.MULTILINE | re.IGNORECASE)
        if org_match:
            extracted_org = org_match.group(1).strip()
            if extracted_org and extracted_org != org_name:
                logger.info(f"Updating organization for '{repo_name}' based on README: '{org_name}' -> '{extracted_org}'")
                repo_data['organization'] = extracted_org
        else:
            logger.debug(f"Repo '{org_name}/{repo_name}' - No organization found. Calling AI analysis.")
            org_name = _call_ai_for_organization(repo_data)
            if org_name:
                repo_data['organization'] = org_name
                logger.info(f"Repo '{repo_name}' linked to {org_name} via AI analysis.")
            else:
                logger.debug(f"Repo '{repo_name}' - AI analysis did not result in an organization name.")

        # Contract processing
        contract_match = re.search(r"^Contract#:\s*(.*)", readme_content, re.MULTILINE | re.IGNORECASE)
        if contract_match:
            repo_data['contractNumber'] = contract_match.group(1).strip()
            logger.debug(f"Found Contract#: {repo_data['contractNumber']} in README for private repo {repo_name}")


    # --- Exemption Cascade Logic ---
    # Step 1: Manual Exemption Check (README.MD)
    # Check if the exemption is already applied in the readme.md file
    # Look for a line starting with "Exemption:" followed by a valid exemption code
    # and a justification line starting with "Exemption justification:"
    if readme_content:
        manual_exempt_match = re.search(r"Exemption:\s*(\S+)", readme_content, re.IGNORECASE | re.MULTILINE)
        justification_match = re.search(r"Exemption justification:\s*(.*)", readme_content, re.IGNORECASE | re.MULTILINE)

        if manual_exempt_match and justification_match:
            captured_code = manual_exempt_match.group(1).strip()
            if captured_code in VALID_AI_EXEMPTION_CODES:
                repo_data['permissions']['usageType'] = captured_code
                repo_data['permissions']['exemptionText'] = justification_match.group(1).strip()
                exemption_applied = True
                logger.info(f"Repo '{org_name}/{repo_name}' - Step 1: Exempted manually via README ({captured_code}).")
            else:
                logger.warning(f"Repo '{org_name}/{repo_name}' - Found manual exemption tag 'Exemption: {captured_code}' in README, but the code is not valid. Ignoring manual exemption.")

    # Step 2: Non-code Detection (Only if not already exempted)
    # Check if the repository is likely non-code based on the language heuristic
    # and apply exemption if applicable
    if not exemption_applied:
        is_likely_non_code = language in NON_CODE_LANGUAGES
        if is_likely_non_code:
            repo_data['permissions']['usageType'] = EXEMPT_NON_CODE
            repo_data['permissions']['exemptionText'] = f"Non-code repository (heuristic based on primary language: {language})"
            exemption_applied = True
            logger.info(f"Repo '{org_name}/{repo_name}' - Step 2: Exempted as non-code (Language: {language}).")

    # Step 3: Sensitive Keyword Detection (README) (Only if not already exempted)
    # Check if the repository contains sensitive keywords in the README.MD content
    # and apply exemption if applicable
    if not exemption_applied and readme_content:
        found_keywords = [kw for kw in SENSITIVE_KEYWORDS if re.search(r'\b' + re.escape(kw) + r'\b', readme_content, re.IGNORECASE)]
        if found_keywords:
            repo_data['permissions']['usageType'] = EXEMPT_BY_LAW # Default to EXEMPT_BY_LAW for keywords
            repo_data['permissions']['exemptionText'] = f"Flagged for review: Found keywords in README: [{', '.join(found_keywords)}]"
            exemption_applied = True
            logger.info(f"Repo '{org_name}/{repo_name}' - Step 3: Exempted due to sensitive keywords in README ({EXEMPT_BY_LAW}): {found_keywords}.")

    # --- Step 4: AI Fallback (Only if not already exempted) ---
    # If no exemption was applied yet, call the AI to determine the usageType
    # and exemptionText based on the repository details
    if not exemption_applied:
        logger.debug(f"Repo '{org_name}/{repo_name}' - Step 4: No standard exemption found. Calling AI analysis.")
        ai_usage_type, ai_exemption_text = _call_ai_for_exemption(repo_data)
        if ai_usage_type:
            repo_data['permissions']['usageType'] = ai_usage_type
            repo_data['permissions']['exemptionText'] = ai_exemption_text
            exemption_applied = True
            logger.info(f"Repo '{org_name}/{repo_name}' - Step 4: Exempted via AI analysis ({ai_usage_type}).")
        else:
            logger.debug(f"Repo '{org_name}/{repo_name}' - Step 4: AI analysis did not result in an exemption.")


    # --- Assign Default usageType if NOT Exempted ---
    if not exemption_applied:
        logger.debug(f"Repo '{org_name}/{repo_name}' - No exemption applied. Assigning default usageType.")
        if is_private:
            repo_data['permissions']['usageType'] = USAGE_GOVERNMENT_WIDE_REUSE
            logger.info(f"Repo '{org_name}/{repo_name}' - Assigned default usageType: {USAGE_GOVERNMENT_WIDE_REUSE} (private repo).")
        else:
            repo_data['permissions']['usageType'] = USAGE_OPEN_SOURCE
            logger.info(f"Repo '{org_name}/{repo_name}' - Assigned default usageType: {USAGE_OPEN_SOURCE} (public repo).")
        # Ensure exemptionText is None if no exemption applied
        repo_data['permissions']['exemptionText'] = None

    # --- README Fallbacks for Missing Schema Fields ---
    if readme_content:
        # Fallback for Version (if still "N/A")
        if repo_data.get("version") == "N/A":
            parsed_version = _parse_readme_for_version(readme_content)
            if parsed_version:
                repo_data["version"] = parsed_version
                logger.info(f"Repo '{org_name}/{repo_name}' - Updated 'version' from README fallback.")

        # Fallback for Tags (if still [])
        # Check if 'tags' key exists and is empty list
        if isinstance(repo_data.get("tags"), list) and not repo_data.get("tags"):
            parsed_tags = _parse_readme_for_tags(readme_content)
            if parsed_tags:
                repo_data["tags"] = parsed_tags
                logger.info(f"Repo '{org_name}/{repo_name}' - Updated 'tags' from README fallback.")

        parsed_status = _parse_readme_for_status(readme_content)
        if parsed_status:
            repo_data["_status_from_readme"] = parsed_status # Store in temporary field
            logger.info(f"Repo '{org_name}/{repo_name}' - Found status '{parsed_status}' from README.")

        # Fallback for License URL (if license exists, URL is missing, AND it's not the default)
        license_list_in_perms = repo_data.get('permissions', {}).get('licenses')
        if (isinstance(license_list_in_perms, list) and
                license_list_in_perms and # Check if list is not empty
                # Check if URL key is missing OR if its value is None/empty
                (license_list_in_perms[0].get('URL') is None or not license_list_in_perms[0].get('URL')) and
                # Check if it's NOT the default license we added
                license_list_in_perms[0].get('name') != "Apache License 2.0"):
             # Basic check: if readme_url exists and contains 'README', try replacing with 'LICENSE'
             readme_url = repo_data.get('readme_url')
             if readme_url and isinstance(readme_url, str):
                  # Very simple heuristic, might need refinement based on platform URL structure
                  potential_license_url = None
                  if 'README.md' in readme_url:
                       potential_license_url = readme_url.replace('README.md', 'LICENSE', 1)
                  elif 'README.txt' in readme_url:
                       potential_license_url = readme_url.replace('README.txt', 'LICENSE', 1)
                  elif '/README' in readme_url: # More generic
                       potential_license_url = readme_url.replace('/README', '/LICENSE', 1)

                  if potential_license_url and potential_license_url != readme_url:
                       # We don't KNOW if this URL is valid, but it's a guess
                       repo_data['permissions']['licenses'][0]['URL'] = potential_license_url
                       logger.info(f"Repo '{org_name}/{repo_name}' - Guessed licence URL: {potential_license_url} based on README URL.")


    # --- Final Contact Email Logic for code.json ---
    # Use the temporary list populated earlier
    final_json_email = None
    if is_private:
        # --- Use Environment Variable ---
        final_json_email = PRIVATE_CONTACT_EMAIL_DEFAULT # Use variable read from env
        logger.debug(f"Setting contact.email to generic '{final_json_email}' for private repo {repo_name}")
    elif all_extracted_emails_temp: # Use the temp variable here
        # Public repo: use the first actual email found (already sorted)
        final_json_email = all_extracted_emails_temp[0]
        logger.info(f"Setting primary contact.email to '{final_json_email}' for public repo {repo_name}")
    else:
        # Public repo with NO emails found: use default public email
        # --- Use Environment Variable ---
        final_json_email = PUBLIC_CONTACT_EMAIL_DEFAULT # Use variable read from env
        logger.debug(f"Setting default public contact email for {repo_name} as none were extracted.")

    # Set the email in the contact dict
    repo_data['contact']['email'] = final_json_email

    # --- Final Cleanup: Remove empty contact dict if truly empty ---
    # If only 'name' remains and email is None (shouldn't happen with defaults, but good check)
    if not repo_data['contact'].get('email') and \
       not all_extracted_emails_temp and \
       list(repo_data.get('contact', {}).keys()) == ['name']: # Check existence of contact dict
         repo_data.pop('contact', None)
         logger.debug(f"Removed empty contact dictionary for {repo_name}")

    # --- Remove temporary fields ---
    repo_data.pop('_codeowners_content', None)
    repo_data.pop('readme_content', None)
    # Remove the temporary list from the final output (no longer needed in contact dict)
    # repo_data['contact'].pop('all_extracted_emails', None) # This was removed in previous step


    return repo_data
