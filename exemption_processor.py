# exemption_processor.py

import re
import logging

logger = logging.getLogger(__name__)

# --- Define Exemption Codes as Constants ---
EXEMPT_BY_LAW = "exemptByLaw"
EXEMPT_NON_CODE = "exemptNonCode" # Changed from "exempt" to be more specific
EXEMPT_BY_NATIONAL_SECURITY = "exemptByNationalSecurity"
EXEMPT_BY_AGENCY_SYSTEM = "exemptByAgencySystem"
EXEMPT_BY_MISSION_SYSTEM = "exemptByMissionSystem"
EXEMPT_BY_CIO = "exemptByCIO"
# --- Define Default Usage Types ---
USAGE_OPEN_SOURCE = "openSource"
USAGE_GOVERNMENT_WIDE_REUSE = "governmentWideReuse"
# --- End Constants ---

# Define sensitive keywords and non-code languages centrally
SENSITIVE_KEYWORDS = ["HIPAA", "PHI", "CUI","PII","Internal use only", "Patient data"]
NON_CODE_LANGUAGES = [None, '', 'Markdown', 'Text', 'HTML', 'CSS', 'Jupyter Notebook']

def process_repository_exemptions(repo_data: dict) -> dict:
    """
    Applies exemption logic cascade and private repo processing.
    Updates repo_data with 'exempted', 'usageType', 'exemptionText',
    and potentially 'contractNumber', 'org_name', 'contact_email'.
    Assigns default usageType if no exemption applies.
    """
    # Initialize exemption fields
    repo_data['exempted'] = False
    repo_data['usageType'] = None
    repo_data['exemptionText'] = None
    repo_data['contractNumber'] = repo_data.get('contractNumber')

    readme_content = repo_data.get('readme_content')
    language = repo_data.get('language')
    is_private = repo_data.get('is_private', False) # Default to False if key missing
    repo_name = repo_data.get('repo_name', 'UnknownRepo')
    org_name = repo_data.get('org_name', 'UnknownOrg')

    logger.debug(f"Processing exemptions/private data for: {org_name}/{repo_name}")

    # --- Private Repository Processing ---
    if is_private and readme_content:
        # (Private repo logic remains the same)
        org_match = re.search(r"^Org:\s*(.*)", readme_content, re.MULTILINE | re.IGNORECASE)
        if org_match:
            extracted_org = org_match.group(1).strip()
            if extracted_org and extracted_org != org_name:
                logger.info(f"Updating org_name for '{repo_name}' based on README: '{org_name}' -> '{extracted_org}'")
                repo_data['org_name'] = extracted_org

        contract_match = re.search(r"^Contract#:\s*(.*)", readme_content, re.MULTILINE | re.IGNORECASE)
        if contract_match:
            repo_data['contractNumber'] = contract_match.group(1).strip()
            logger.debug(f"Found Contract#: {repo_data['contractNumber']} in README for private repo {repo_name}")

        email_match = re.search(r"^Email Requests:", readme_content, re.MULTILINE | re.IGNORECASE)
        if email_match:
            repo_data['contact_email'] = "shareit@cdc.gov"
            logger.debug(f"Found 'Email Requests:' in README for private repo {repo_name}. Setting contact email to default.")

    # --- Exemption Cascade Logic ---

    # Step 1: Manual Exemption Check (README)
    if readme_content:
        manual_exempt_match = re.search(r"Exemption:\s*" + re.escape(EXEMPT_BY_LAW), readme_content, re.IGNORECASE | re.MULTILINE)
        justification_match = re.search(r"Exemption justification:\s*(.*)", readme_content, re.IGNORECASE | re.MULTILINE)

        if manual_exempt_match and justification_match:
            repo_data['exempted'] = True
            repo_data['usageType'] = EXEMPT_BY_LAW
            repo_data['exemptionText'] = justification_match.group(1).strip()
            logger.info(f"Repo '{org_name}/{repo_name}' - Step 1: Exempted manually via README ({EXEMPT_BY_LAW}).")
            return repo_data

    # Step 2: Non-code Detection
    is_likely_non_code = language in NON_CODE_LANGUAGES
    if is_likely_non_code:
        repo_data['exempted'] = True
        repo_data['usageType'] = EXEMPT_NON_CODE
        repo_data['exemptionText'] = f"Non-code repository (heuristic based on primary language: {language})"
        logger.info(f"Repo '{org_name}/{repo_name}' - Step 2: Exempted as non-code (Language: {language}).")
        return repo_data

    # Step 3: Sensitive Keyword Detection (README)
    if readme_content:
        found_keywords = [
            keyword for keyword in SENSITIVE_KEYWORDS
            if re.search(r'\b' + re.escape(keyword) + r'\b', readme_content, re.IGNORECASE)
        ]
        if found_keywords:
            repo_data['exempted'] = True
            repo_data['usageType'] = EXEMPT_BY_LAW
            repo_data['exemptionText'] = f"Flagged for review: Found keywords in README: [{', '.join(found_keywords)}]"
            logger.info(f"Repo '{org_name}/{repo_name}' - Step 3: Exempted due to sensitive keywords in README ({EXEMPT_BY_LAW}): {found_keywords}.")
            return repo_data

    # Step 4: AI Fallback (Placeholder)
    # ... (AI logic would go here and potentially return if exemption found) ...

    # --- Assign Default usageType if NOT Exempted ---
    if not repo_data['exempted']:
        logger.debug(f"Repo '{org_name}/{repo_name}' - No exemption applied. Assigning default usageType.")
        if is_private:
            repo_data['usageType'] = USAGE_GOVERNMENT_WIDE_REUSE
            logger.info(f"Repo '{org_name}/{repo_name}' - Assigned default usageType: {USAGE_GOVERNMENT_WIDE_REUSE} (private repo).")
        else:
            repo_data['usageType'] = USAGE_OPEN_SOURCE
            logger.info(f"Repo '{org_name}/{repo_name}' - Assigned default usageType: {USAGE_OPEN_SOURCE} (public repo).")
        # No exemptionText is needed for default usage types

    return repo_data
