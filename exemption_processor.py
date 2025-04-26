# exemption_processor.py

import re
import logging
import os
from dotenv import load_dotenv
import time

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

# --- AI Configuration ---
if AI_ENABLED:
    load_dotenv()
    GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
    if GOOGLE_API_KEY:
        try:
            genai.configure(api_key=GOOGLE_API_KEY)
            logger.info("Google Generative AI configured successfully.")
            logger.info(f"AI processing status after configuration: {AI_ENABLED}")
        except Exception as ai_config_err:
            logger.error(f"Failed to configure Google Generative AI: {ai_config_err}", exc_info=True)
            AI_ENABLED = False
            logger.warning(f"AI processing status after configuration failure: {AI_ENABLED}")
    else:
        logger.warning("GOOGLE_API_KEY environment variable not found. AI processing will be disabled.")
        AI_ENABLED = False
        logger.info(f"AI processing status (API key missing): {AI_ENABLED}")

# --- Helper Function for AI Call ---
def _call_ai_for_exemption(repo_data: dict) -> tuple[str | None, str | None]:
    # ... (AI call logic remains the same) ...
    if not AI_ENABLED or not genai:
        logger.debug("AI processing is disabled (library not imported or key missing/invalid). Skipping AI call.")
        return None, None
    readme = repo_data.get('readme_content', '') or ''
    description = repo_data.get('description', '') or ''
    repo_name = repo_data.get('repo_name', '')
    if not readme.strip() and not description.strip():
        logger.debug(f"No significant text content (README/description) found for AI analysis of '{repo_name}'. Skipping AI call.")
        return None, None
    max_input_length = 15000
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
        model = genai.GenerativeModel('gemini-1.5-flash-latest')
        response = model.generate_content(
            prompt,
            generation_config=genai.types.GenerationConfig(temperature=0.1),
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
        logger.error(f"Error during AI call for repository '{repo_name}': {ai_err}", exc_info=True)
        return None, None
    finally:
        delay_seconds = 4.5
        logger.debug(f"Pausing for {delay_seconds} seconds to respect AI rate limit...")
        time.sleep(delay_seconds)


def process_repository_exemptions(repo_data: dict) -> dict:
    """
    Applies exemption logic cascade (including AI fallback) and private repo processing.
    Updates repo_data with 'exempted', 'usageType', 'exemptionText', 'contact',
    and potentially 'contractNumber', 'org_name'.
    Assigns default usageType if no exemption applies.
    """
    # Initialize fields
    repo_data['exempted'] = False
    repo_data['usageType'] = None
    repo_data['exemptionText'] = None
    repo_data['contractNumber'] = repo_data.get('contractNumber')
    repo_data['contact'] = {} # Initialize contact dictionary

    readme_content = repo_data.get('readme_content')
    language = repo_data.get('language')
    is_private = repo_data.get('is_private', False)
    repo_name = repo_data.get('repo_name', 'UnknownRepo')
    org_name = repo_data.get('org_name', 'UnknownOrg')

    logger.debug(f"Processing exemptions/private data for: {org_name}/{repo_name}")

    # --- Private Repository Processing (Org, Contract, Default Email) ---
    if is_private and readme_content:
        logger.debug(f"Applying private repo README parsing for: {org_name}/{repo_name}")
        # Org processing
        org_match = re.search(r"^Org:\s*(.*)", readme_content, re.MULTILINE | re.IGNORECASE)
        if org_match:
            extracted_org = org_match.group(1).strip()
            if extracted_org and extracted_org != org_name:
                logger.info(f"Updating org_name for '{repo_name}' based on README: '{org_name}' -> '{extracted_org}'")
                repo_data['org_name'] = extracted_org

        # Contract processing
        contract_match = re.search(r"^Contract#:\s*(.*)", readme_content, re.MULTILINE | re.IGNORECASE)
        if contract_match:
            repo_data['contractNumber'] = contract_match.group(1).strip()
            logger.debug(f"Found Contract#: {repo_data['contractNumber']} in README for private repo {repo_name}")

        # Default Email for Private Repos if specified
        email_match = re.search(r"^Email Requests:", readme_content, re.MULTILINE | re.IGNORECASE)
        if email_match:
            repo_data['contact']['email'] = "shareit@cdc.gov" # Set default email in contact dict
            logger.debug(f"Found 'Email Requests:' in README for private repo {repo_name}. Setting contact.email to default.")

    # --- Exemption Cascade Logic ---
    # Step 1: Manual Exemption Check (README)
    if readme_content:
        # Looks for "Exemption:", optional whitespace, then captures non-whitespace characters (\S+)
        manual_exempt_match = re.search(r"Exemption:\s*(\S+)", readme_content, re.IGNORECASE | re.MULTILINE)
        # Justification regex remains the same
        justification_match = re.search(r"Exemption justification:\s*(.*)", readme_content, re.IGNORECASE | re.MULTILINE)
        
        # Check if both matches were found
        if manual_exempt_match and justification_match:
            # --- Extract and Validate the captured code ---
            captured_code = manual_exempt_match.group(1).strip()
            # Check if the captured code is one of the known valid codes
            # (Using VALID_AI_EXEMPTION_CODES list as it contains the main ones)
            if captured_code in VALID_AI_EXEMPTION_CODES:
                repo_data['exempted'] = True
                repo_data['usageType'] = captured_code # Use the captured code
                repo_data['exemptionText'] = justification_match.group(1).strip()
                logger.info(f"Repo '{org_name}/{repo_name}' - Step 1: Exempted manually via README ({captured_code}).")
                # Clean up contact dict if empty before returning
                if not repo_data['contact']: repo_data.pop('contact', None)
                return repo_data
            else:
                # Log if an invalid code was found after "Exemption:"
                logger.warning(f"Repo '{org_name}/{repo_name}' - Found manual exemption tag 'Exemption: {captured_code}' in README, but the code is not valid. Ignoring manual exemption.")

    # Step 2: Non-code Detection
    is_likely_non_code = language in NON_CODE_LANGUAGES
    if is_likely_non_code:
        repo_data['exempted'] = True
        repo_data['usageType'] = EXEMPT_NON_CODE
        repo_data['exemptionText'] = f"Non-code repository (heuristic based on primary language: {language})"
        logger.info(f"Repo '{org_name}/{repo_name}' - Step 2: Exempted as non-code (Language: {language}).")
        # Clean up contact dict if empty before returning
        if not repo_data['contact']: repo_data.pop('contact', None)
        return repo_data

    # Step 3: Sensitive Keyword Detection (README)
    if readme_content:
        found_keywords = [kw for kw in SENSITIVE_KEYWORDS if re.search(r'\b' + re.escape(kw) + r'\b', readme_content, re.IGNORECASE)]
        if found_keywords:
            repo_data['exempted'] = True
            repo_data['usageType'] = EXEMPT_BY_LAW
            repo_data['exemptionText'] = f"Flagged for review: Found keywords in README: [{', '.join(found_keywords)}]"
            logger.info(f"Repo '{org_name}/{repo_name}' - Step 3: Exempted due to sensitive keywords in README ({EXEMPT_BY_LAW}): {found_keywords}.")
            # Clean up contact dict if empty before returning
            if not repo_data['contact']: repo_data.pop('contact', None)
            return repo_data

    # --- Step 4: AI Fallback ---
    if not repo_data['exempted']:
        logger.debug(f"Repo '{org_name}/{repo_name}' - Step 4: No standard exemption found. Calling AI analysis.")
        ai_usage_type, ai_exemption_text = _call_ai_for_exemption(repo_data)
        if ai_usage_type:
            repo_data['exempted'] = True
            repo_data['usageType'] = ai_usage_type
            repo_data['exemptionText'] = ai_exemption_text
            logger.info(f"Repo '{org_name}/{repo_name}' - Step 4: Exempted via AI analysis ({ai_usage_type}).")
            # Clean up contact dict if empty before returning
            if not repo_data['contact']: repo_data.pop('contact', None)
            return repo_data
        else:
            logger.debug(f"Repo '{org_name}/{repo_name}' - Step 4: AI analysis did not result in an exemption.")

    # --- Assign Default usageType if NOT Exempted ---
    if not repo_data['exempted']:
        logger.debug(f"Repo '{org_name}/{repo_name}' - No exemption applied. Assigning default usageType.")
        if is_private:
            repo_data['usageType'] = USAGE_GOVERNMENT_WIDE_REUSE
            logger.info(f"Repo '{org_name}/{repo_name}' - Assigned default usageType: {USAGE_GOVERNMENT_WIDE_REUSE} (private repo).")
        else:
            repo_data['usageType'] = USAGE_OPEN_SOURCE
            logger.info(f"Repo '{org_name}/{repo_name}' - Assigned default usageType: {USAGE_OPEN_SOURCE} (public repo).")

    # --- Add Extracted Emails for PUBLIC Repos ---
    if not is_private and readme_content:
        logger.debug(f"Checking for contact emails in README for public repo: {org_name}/{repo_name}")
        email_match = re.search(r"^Email Requests:\s*(.*)", readme_content, re.MULTILINE | re.IGNORECASE)
        if email_match:
            extracted_emails_str = email_match.group(1).strip()
            cleaned_emails = [
                email.strip()
                for email in re.split(r'[;, ]+', extracted_emails_str.replace('mailto:', ''))
                if '@' in email and '.' in email # Basic validation
            ]
            if cleaned_emails:
                # Add emails to the contact dictionary
                repo_data['contact']['emails'] = ','.join(cleaned_emails)
                logger.debug(f"Added extracted contact emails '{repo_data['contact']['emails']}' to contact.emails for public repo {repo_name}")
            else:
                logger.debug(f"Found 'Email Requests:' line but no valid emails extracted for public repo {repo_name}.")
        else:
             logger.debug(f"No 'Email Requests:' line found in README for public repo {repo_name}.")

    # --- Final Cleanup: Remove empty contact dict ---
    if not repo_data['contact']:
        repo_data.pop('contact', None)

    return repo_data
