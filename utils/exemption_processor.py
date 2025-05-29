# utils/exemption_processor.py
"""
Handles the core logic for processing repository metadata to determine
code-sharing exemptions, infer missing information, and prepare data for
the final code.json schema.

This module applies a cascade of rules and heuristics:
- Loads configuration (AI settings, default emails) from environment variables.
- Optionally initializes and utilizes a Generative AI model (Google Gemini)
  if configured and available.
- Defines standard exemption codes and non-code language identifiers.
- Extracts contact emails from README and CODEOWNERS content.
- Parses README content for specific markers (manual exemptions, version,
  tags, status, organization, contract number).
- Implements an exemption determination logic:
    1. Checks for manually specified exemptions in the README.
    2. Identifies repositories likely containing only non-code content based
       on detected languages.
    3. Scans README content for sensitive keywords indicating potential
       exemption needs (e.g., EXEMPT_BY_LAW).
    4. (If AI enabled) Uses AI as a fallback to suggest an exemption code and
       justification based on repository name, description, and README.
    5. (If AI enabled) Uses AI to suggest an owning organization name if not
       explicitly found.
- Assigns default usage types (`openSource` or `governmentWideReuse`) if no
  exemption is applied.
- Uses parsed README information as a fallback for missing schema fields like
  'version', 'tags', and potentially 'organization' or 'contractNumber'.
- Determines the final 'contact.email' field based on repository visibility
  (private vs. public), extracted emails, and configured defaults.
- Cleans up temporary processing fields before returning the updated repository
  data dictionary.
"""

import re
import html
import logging
import os
from dotenv import load_dotenv
import time
from typing import List, Optional, Dict, Any

from .config import Config # For type hinting cfg_obj

# --- SSL Verification Control & urllib3 Warning Suppression ---
import warnings
try:
    from urllib3.exceptions import InsecureRequestWarning
except ImportError:
    # Define a dummy if urllib3 is not available, though it's a deep dependency of requests
    class InsecureRequestWarning(Warning): # type: ignore
        pass

# --- Try importing the AI library ---
try:
    import google.generativeai as genai
    from google.api_core import exceptions as google_api_exceptions
    AI_LIBRARY_IMPORTED = True # Indicates the library itself is available
except ImportError:
    AI_LIBRARY_IMPORTED = False
    InvalidArgument, PermissionDenied = None, None # Define for type hinting and checks
    genai = None # Ensure genai is defined even if import fails

logger = logging.getLogger(__name__)

# For catching requests.exceptions.SSLError if underlying auth uses it, or for other SSL errors
try:
    import requests
except ImportError:
    requests = None # type: ignore

logger.info(f"Initial AI library import status (google.generativeai): {AI_LIBRARY_IMPORTED}")

# ANSI escape codes for coloring output
ANSI_RED = "\x1b[31;1m"
ANSI_YELLOW = "\x1b[33;1m"
ANSI_RESET = "\x1b[0m"

# --- Define Exemption Codes as Constants ---
EXEMPT_BY_LAW = "exemptByLaw"
EXEMPT_NON_CODE = "exemptNonCode"
EXEMPT_BY_NATIONAL_SECURITY = "exemptByNationalSecurity"
EXEMPT_BY_AGENCY_SYSTEM = "exemptByAgencySystem"
EXEMPT_BY_MISSION_SYSTEM = "exemptByMissionSystem"
EXEMPT_BY_CIO = "exemptByCIO"
USAGE_OPEN_SOURCE = "openSource"
USAGE_GOVERNMENT_WIDE_REUSE = "governmentWideReuse"

VALID_AI_EXEMPTION_CODES = [
    EXEMPT_BY_LAW, EXEMPT_BY_NATIONAL_SECURITY, EXEMPT_BY_AGENCY_SYSTEM,
    EXEMPT_BY_MISSION_SYSTEM, EXEMPT_BY_CIO,
]
SENSITIVE_KEYWORDS = ["HIPAA", "PHI", "CUI", "PII", "Internal use only", "Patient data"]
NON_CODE_LANGUAGES = [
    None, '', 'Markdown', 'Text', 'HTML', 'CSS', 'XML', 'YAML', 'JSON',
    'Shell', 'Batchfile', 'PowerShell', 'Dockerfile', 'Makefile', 'CMake',
    'TeX', 'Roff', 'CSV', 'TSV'
]

load_dotenv()
# MAX_TOKENS_ENV is for input truncation, will be passed in
# AI_TEMPERATURE_ENV will be passed in
# AI_MODEL_NAME_ENV will be passed in
# AI_MAX_OUTPUT_TOKENS_ENV will be passed in


KNOWN_CDC_ORGANIZATIONS = {
    "cdc": "Centers for Disease Control and Prevention", "od": "Office of the Director",
    "om": "Office of Mission Support", "ocoo": "Office of the Chief Operating Officer",
    "oadc": "Office of the Associate Directory of Communications",
    "ocio": "Office of the Chief Information Officer",
    "oed": "Office of Equal Employment Opportunity and Workplace Equity",
    "oga": "Office of Global Affairs", "ohs": "Office of Health Equity",
    "opa": "Office of Policy, Performance, and Evaluation",
    "ostlts": "Office of State, Tribal, Local and Territorial Support",
    "owcd": "Office of Women’s Health and Health Equity",
    "cSELS": "Center for Surveillance, Epidemiology, and Laboratory Services",
    "csels": "Center for Surveillance, Epidemiology, and Laboratory Services",
    "ddphss": "Deputy Director for Public Health Science and Surveillance",
    "cgH": "Center for Global Health", "cgh": "Center for Global Health",
    "cid": "Center for Preparedness and Response", "cpr": "Center for Preparedness and Response",
    "ncezid": "National Center for Emerging and Zoonotic Infectious Diseases",
    "ncird": "National Center for Immunization and Respiratory Diseases",
    "nchhstp": "National Center for HIV, Viral Hepatitis, STD, and TB Prevention",
    "nccdphp": "National Center for Chronic Disease Prevention and Health Promotion",
    "nceh": "National Center for Environmental Health",
    "atsdr": "Agency for Toxic Substances and Disease Registry",
    "ncipc": "National Center for Injury Prevention and Control",
    "ncbddd": "National Center on Birth Defects and Developmental Disabilities",
    "nchs": "National Center for Health Statistics",
    "niosh": "National Institute for Occupational Safety and Health",
    "ddid": "Deputy Director for Infectious Diseases",
    "ddnidd": "Deputy Director for Non-Infectious Diseases",
    "cfa": "Center for Forecasting and Outbreak Analytics",
    "ophdst": "Office of Public Health Data, Surveillance, and Technology",
    "amd": "Office of Advanced Molecular Detection", "oamd": "Office of Advanced Molecular Detection",
}

AI_DELAY_ENABLED = float(os.getenv("AI_DELAY_ENABLED", 0.0))
logger.info(f"Using AI_DELAY_ENABLED value: {AI_DELAY_ENABLED}")

AI_ORGANIZATION_ENABLED = os.getenv("AI_ORGANIZATION_ENABLED", "False").lower() == "true"
logger.info(f"AI Organization Inference Enabled: {AI_ORGANIZATION_ENABLED}")

PRIVATE_CONTACT_EMAIL_DEFAULT = os.getenv("PRIVATE_REPO_CONTACT_EMAIL", "shareit@cdc.gov")
PUBLIC_CONTACT_EMAIL_DEFAULT = os.getenv("DEFAULT_CONTACT_EMAIL", "shareit@cdc.gov")
logger.info(f"Using Private Repo Contact Email: {PRIVATE_CONTACT_EMAIL_DEFAULT}")
logger.info(f"Using Default Public Contact Email: {PUBLIC_CONTACT_EMAIL_DEFAULT}")

# --- AI Configuration ---
# This global flag will now reflect the combination of API key validity AND the passed-in config.
_MODULE_AI_ENABLED_STATUS = False # Internal status reflecting API key validity and library import
PLACEHOLDER_GOOGLE_API_KEY = "YOUR_GOOLE_API_KEY"

# --- SSL Verification Check and urllib3 Warning Suppression (Module Level) ---
DISABLE_SSL_ENV = os.getenv("DISABLE_SSL_VERIFICATION", "false").lower()
if DISABLE_SSL_ENV == "true":
    logger.warning(f"{ANSI_RED}SECURITY WARNING: Global DISABLE_SSL_VERIFICATION is true.{ANSI_RESET}")
    logger.warning(f"{ANSI_YELLOW}This may suppress urllib3 InsecureRequestWarnings if the AI client or its auth libraries use HTTPS (non-gRPC) for some operations.{ANSI_RESET}")
    logger.warning(f"{ANSI_YELLOW}IMPORTANT: This flag DOES NOT disable SSL/TLS certificate verification for gRPC calls made by the Google AI SDK.{ANSI_RESET}")
    logger.warning(f"{ANSI_YELLOW}If you encounter 'CERTIFICATE_VERIFY_FAILED' errors specifically from Google AI services, you must ensure your system's trust store includes the necessary CA certificates for Google's domains (or any intercepting proxy).{ANSI_RESET}")
    logger.warning(f"{ANSI_YELLOW}As a consequence of DISABLE_SSL_VERIFICATION=true, AI-driven exemption and organization processing will be SKIPPED to avoid potential SSL errors with AI services.{ANSI_RESET}")
    try:
        warnings.filterwarnings('ignore', category=InsecureRequestWarning)
        logger.info("Suppressed urllib3.exceptions.InsecureRequestWarning globally due to DISABLE_SSL_VERIFICATION=true.")
    except Exception as e_warn_filter:
        logger.warning(f"Could not suppress InsecureRequestWarning: {e_warn_filter}")


if AI_LIBRARY_IMPORTED: # Only proceed if the google.generativeai library was successfully imported
    GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
    if not GOOGLE_API_KEY:
        logger.warning("GOOGLE_API_KEY environment variable not found. AI processing will be disabled for this module.")
        _MODULE_AI_ENABLED_STATUS = False
    elif GOOGLE_API_KEY == PLACEHOLDER_GOOGLE_API_KEY:
        logger.warning(f"GOOGLE_API_KEY is set to a placeholder value ('{PLACEHOLDER_GOOGLE_API_KEY}'). AI processing will be disabled for this module.")
        _MODULE_AI_ENABLED_STATUS = False
    else:
        try:
            if genai: # Ensure genai is not None (it wouldn't be if AI_LIBRARY_IMPORTED is True)
                genai.configure(api_key=GOOGLE_API_KEY)
                logger.info("Google Generative AI configured successfully with the provided API key.")
                
                # Add SSL connectivity test here
                try:
                    import socket
                    import ssl
                    hostname = "generativelanguage.googleapis.com"
                    port = 443
                    sock = socket.create_connection((hostname, port), timeout=5)
                    context = ssl.create_default_context()
                    with context.wrap_socket(sock, server_hostname=hostname) as ssock:
                        logger.info("SSL connectivity test to Google AI API passed.")
                        _MODULE_AI_ENABLED_STATUS = True # Module *can* use AI if enabled by config
                except (socket.timeout, socket.error, ssl.SSLError, ConnectionError) as ssl_err:
                    logger.error(f"{ANSI_RED}SSL connectivity test failed for Google AI API: {ssl_err}. AI processing will be disabled to prevent hangs.{ANSI_RESET}")
                    _MODULE_AI_ENABLED_STATUS = False
                except Exception as ssl_test_err:
                    logger.warning(f"Unexpected error during SSL connectivity test: {ssl_test_err}. AI processing will be disabled as a precaution.")
                    _MODULE_AI_ENABLED_STATUS = False
                    
            else: # Should not happen if AI_LIBRARY_IMPORTED is True
                logger.error("Google Generative AI library was marked as imported, but 'genai' module is None. AI processing disabled.")
                _MODULE_AI_ENABLED_STATUS = False
                
        except Exception as ai_config_err:
            # Check if the configuration error is due to an invalid API key
            err_str = str(ai_config_err).lower()
            if "api key" in err_str and ("invalid" in err_str or "not valid" in err_str):
                logger.error(f"{ANSI_RED}Failed to configure Google Generative AI: API key is not valid. AI processing will be disabled.{ANSI_RESET} Error: {ai_config_err}")
            else:
                logger.error(f"{ANSI_RED}Failed to configure Google Generative AI with the provided API key: {ai_config_err}{ANSI_RESET}")
            _MODULE_AI_ENABLED_STATUS = False
else:
    logger.info("Google Generative AI library not imported. AI processing will be disabled for this module.")
    _MODULE_AI_ENABLED_STATUS = False
logger.info(f"Module-level AI readiness (API key & library): {_MODULE_AI_ENABLED_STATUS}")


# --- Marker Regular Expressions ---
VERSION_MARKER = re.compile(r"^\s*Version:\s*(.+)$", re.IGNORECASE | re.MULTILINE) # type: ignore
KEYWORDS_MARKER = re.compile(r"^\s*Keywords:\s*(.+)$", re.IGNORECASE | re.MULTILINE)
ORGANIZATION_MARKER = re.compile(r"^\s*Organization:\s*(.+)$", re.IGNORECASE | re.MULTILINE)
STATUS_REGEX = re.compile(r"^(?:Project Status|Status):\s*(Maintained|Deprecated|Experimental|Active|Inactive)\b", re.MULTILINE | re.IGNORECASE)
LABOR_HOURS_REGEX = re.compile(r"^(?:Estimated Labor Hours|Labor Hours):\s*(\d+)\b", re.MULTILINE | re.IGNORECASE)
CONTACT_LINE_REGEX = re.compile(r"^(?:Contact|Contacts):\s*(.*)", re.MULTILINE | re.IGNORECASE)
HTML_TAG_REGEX = re.compile(r'<[^>]+>')
TAGS_REGEX = re.compile(r"^(?:Keywords|Tags|Topics):\s*(.+)", re.MULTILINE | re.IGNORECASE)
EMAIL_PATTERN = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'


def _programmatic_org_from_repo_name(repo_name: str, current_org: str, default_org_identifiers: list[str], org_group_context_for_log: str) -> str | None:
    if not repo_name or not default_org_identifiers:
        return None
    can_override = any(current_org.lower() == default_id.lower() for default_id in default_org_identifiers)
    if not can_override and current_org and current_org.lower() != "unknownorg":
        return None

    repo_name_lower = repo_name.lower()
    sorted_known_orgs = sorted(KNOWN_CDC_ORGANIZATIONS.items(), key=lambda item: len(item[0]), reverse=True)

    for acronym, full_name in sorted_known_orgs:
        acronym_lower = acronym.lower()
        pattern = rf"(?:^|[^a-z0-9]){re.escape(acronym_lower)}(?:[^a-z0-9]|$)"
        if re.search(pattern, repo_name_lower):
            logger.info(f"Identified organization '{full_name}' from repo name '{repo_name}'. Initial '{current_org}'.", extra={'org_group': org_group_context_for_log})
            return full_name
    return None

def _call_ai_for_organization(
    repo_data: dict,
    cfg_obj: Config, # Changed to accept Config object
    org_group_context_for_log: str,
) -> str | None:
    global _MODULE_AI_ENABLED_STATUS 

    if not cfg_obj.AI_ENABLED_ENV: # Check global AI enable flag from config
        logger.debug("AI processing is globally disabled in .env. Skipping AI organization call.", extra={'org_group': org_group_context_for_log})
        return None
    if cfg_obj.AI_AUTO_DISABLED_SSL_ERROR:
        logger.warning(f"{ANSI_YELLOW}AI features were auto-disabled due to a previous SSL certificate error. Skipping AI organization call for '{repo_data.get('name', 'UnknownRepo')}'.{ANSI_RESET}", extra={'org_group': org_group_context_for_log})
        return None
    if not _MODULE_AI_ENABLED_STATUS or not genai or not cfg_obj.AI_ORGANIZATION_ENABLED_ENV: # Check module status and specific org inference enable
        logger.debug("AI processing, AI organization inference is disabled. Skipping AI organization call.", extra={'org_group': org_group_context_for_log})
        return None

    repo_name_for_ai = repo_data.get('name', '')
    description_for_ai = repo_data.get('description', '')
    tags_list = repo_data.get('tags', [])
    tags_for_ai = ', '.join(map(str,tags_list)) if tags_list else '' # Ensure tags are strings
    readme_content_for_ai = repo_data.get('readme_content', '') or ''
    max_input_tokens_for_readme = cfg_obj.MAX_TOKENS_ENV # Get from cfg_obj
    
    if DISABLE_SSL_ENV == "true":
        logger.warning(f"AI organization call for '{repo_name_for_ai}' skipped because DISABLE_SSL_VERIFICATION is true.", extra={'org_group': org_group_context_for_log})
        return None

    # Reserve some tokens for the prompt structure and expected AI response
    effective_max_readme_len = max_input_tokens_for_readme - 1500 
    if len(readme_content_for_ai) > effective_max_readme_len:
        readme_content_for_ai = readme_content_for_ai[:effective_max_readme_len] + "\n... [README Content Truncated]"
        logger.warning(f"README content for AI organization analysis of '{repo_name_for_ai}' was truncated to fit token limit.", extra={'org_group': org_group_context_for_log})

    if not readme_content_for_ai.strip() and not description_for_ai.strip() and not repo_name_for_ai.strip():
        logger.debug(f"No significant text content (README/description/name) found for AI analysis of '{repo_name_for_ai}'. Skipping AI organization call.", extra={'org_group': org_group_context_for_log})
        return None

    org_list_for_prompt = "\n".join([f"{acronym} = {name}" for acronym, name in KNOWN_CDC_ORGANIZATIONS.items()])
    prompt = f""" 
Your task is to identify the official CDC organizational unit mentioned in the repository text.
You will be given repository information (name, description, tags, README) and a list of known CDC organizations with their acronyms.
Your primary goal is to match this information to one of the known CDC organizations.

Key Instructions:
1.  **Prioritize Acronyms in Repository Name:** If the 'Repository Name' (e.g., "csels-hub", "ocio-project") contains an acronym that clearly matches an entry in the 'Known CDC Organizations' list, this is a strong indicator. You should confidently use this match to determine the organization, especially if the description and README are generic or do not provide conflicting specific organizational information. For example, if 'Repository Name' is "csels-datahub", the organization is "Center for Surveillance, Epidemiology, and Laboratory Services".
2.  **Handle Misspellings:** Be alert to minor misspellings in any text when comparing against the known list. For example, "enter for Surveillance, Epidemiology, and Laboratory Services" should be matched to "Center for Surveillance, Epidemiology, and Laboratory Services".
3.  **Use Full Context:** If the repository name is not definitive or lacks a clear acronym, analyze the description, tags, and README content for mentions of organizational units or related keywords.
4.  **Output Format:**
    *   If a confident match to an organization in the 'Known CDC Organizations' list is found (based on name, acronym, or other text, including corrected misspellings), output the *full official name* from the list.
    *   If, after careful analysis of all provided information, no reasonable match can be made to any organization in the list, output ONLY the word "None".

Known CDC Organizations (Acronym = Full Name):
{org_list_for_prompt}

Repository Information:
Repository Name: {repo_name_for_ai}
Repository Description: {description_for_ai}
Repository Tags: {tags_for_ai}
README Content (excerpt):
---
{readme_content_for_ai}
---
Determine the organization based on the rules above.
    """
    try: # sourcery skip: extract-method
        logger.info(f"Calling AI model '{cfg_obj.AI_MODEL_NAME_ENV}' to infer organization for repository '{repo_name_for_ai}'...", extra={'org_group': org_group_context_for_log})
        model = genai.GenerativeModel(cfg_obj.AI_MODEL_NAME_ENV)
        response = model.generate_content(
            prompt,
            generation_config=genai.types.GenerationConfig(
                temperature=cfg_obj.AI_TEMPERATURE_ENV,
                max_output_tokens=cfg_obj.AI_MAX_OUTPUT_TOKENS_ENV
            ),
        )
        ai_result_text = response.text.strip()
        logger.debug(f"AI raw response for '{repo_name_for_ai}': {ai_result_text}", extra={'org_group': org_group_context_for_log})

        if ai_result_text.lower() == "none":
            logger.info(f"AI analysis for '{repo_name_for_ai}' determined no specific organization name was inferred.", extra={'org_group': org_group_context_for_log})
            return None
        organization = ai_result_text.strip()
        if organization:
            logger.info(f"AI analysis for '{repo_name_for_ai}' suggests an organization: {organization}", extra={'org_group': org_group_context_for_log})
            return organization
        else:
            logger.warning(f"AI analysis for '{repo_name_for_ai}' could not find the organization name. Ignoring.", extra={'org_group': org_group_context_for_log})
            return None
    except (google_api_exceptions.InvalidArgument, google_api_exceptions.PermissionDenied) as ai_auth_err:
        err_str = str(ai_auth_err).lower()
        if "api key not valid" in err_str or "api_key_invalid" in err_str or "permission_denied" in err_str:
            logger.error(
                f"{ANSI_RED}Error during AI organization call for repository '{repo_name_for_ai}': API key is invalid or lacks permissions. "
                f"Disabling AI for the rest of this run. Error: {ai_auth_err}{ANSI_RESET}"
            , extra={'org_group': org_group_context_for_log})
            _MODULE_AI_ENABLED_STATUS = False # Disable at module level if key is bad
        else:
            logger.error(f"Authorization/Argument error during AI organization call for '{repo_name_for_ai}': {ai_auth_err}", extra={'org_group': org_group_context_for_log})
        return None
    except (requests.exceptions.SSLError if requests else None, google_api_exceptions.GoogleAPICallError) as ssl_like_error:
        error_message_lower = str(ssl_like_error).lower()
        is_ssl_error = (
            "ssl" in error_message_lower or
            "certificate" in error_message_lower or
            "tlsv1 alert" in error_message_lower or
            "handshake failed" in error_message_lower or
            (isinstance(ssl_like_error, google_api_exceptions.ServiceUnavailable) and "unavailable" in error_message_lower) # ServiceUnavailable can wrap SSL
        )
        if is_ssl_error:
            logger.error(
                f"{ANSI_RED}SSL/Network Error during AI organization call for '{repo_name_for_ai}': {ssl_like_error}. "
                f"AI features will be auto-disabled for the rest of this run. "
                f"Please check your corporate network's CA certificate setup in the Docker container (see README).{ANSI_RESET}",
                extra={'org_group': org_group_context_for_log}
            )
            cfg_obj.AI_AUTO_DISABLED_SSL_ERROR = True
        else: # Not an SSL error, but still a service/network issue
            logger.error(f"A non-SSL network/service error occurred during AI organization call for '{repo_name_for_ai}': {ssl_like_error}", exc_info=True, extra={'org_group': org_group_context_for_log})
        return None
    except Exception as ai_err:
        logger.error(f"Error during AI call for repository '{repo_name_for_ai}': {ai_err}", extra={'org_group': org_group_context_for_log})
        return None
    finally:
        if _MODULE_AI_ENABLED_STATUS and cfg_obj.AI_DELAY_ENABLED_ENV > 0: # Use delay from cfg_obj
            logger.debug(f"Pausing for {cfg_obj.AI_DELAY_ENABLED_ENV} seconds to respect AI rate limit...", extra={'org_group': org_group_context_for_log})
            time.sleep(cfg_obj.AI_DELAY_ENABLED_ENV)

def _call_ai_for_exemption(
    repo_data: dict,
    cfg_obj: Config, # Changed to accept Config object
    org_group_context_for_log: str,
) -> tuple[str | None, str | None]:
    global _MODULE_AI_ENABLED_STATUS
    repo_name_for_log = repo_data.get('name', 'UnknownRepo')

 
    if not cfg_obj.AI_ENABLED_ENV: # Check global AI enable flag from config
        logger.debug("AI processing is globally disabled in .env. Skipping AI exemption call.", extra={'org_group': org_group_context_for_log})
        return None, None
    if cfg_obj.AI_AUTO_DISABLED_SSL_ERROR:
        logger.warning(f"{ANSI_YELLOW}AI features were auto-disabled due to a previous SSL certificate error. Skipping AI exemption call for '{repo_name_for_log}'.{ANSI_RESET}", extra={'org_group': org_group_context_for_log})
        return None, None
    if not _MODULE_AI_ENABLED_STATUS or not genai: # Check module status
        logger.debug("AI processing is disabled. Skipping AI exemption call.", extra={'org_group': org_group_context_for_log})
        return None, None

    if DISABLE_SSL_ENV == "true":
        logger.warning(f"AI exemption call for '{repo_name_for_log}' skipped because DISABLE_SSL_VERIFICATION is true.", extra={'org_group': org_group_context_for_log})
        return None, None

    readme = repo_data.get('readme_content', '') or ''
    description = repo_data.get('description', '') or ''
    repo_name = repo_data.get('name', '')
    max_input_tokens_for_combined_text = cfg_obj.MAX_TOKENS_ENV # Get from cfg_obj

    if not readme.strip() and not description.strip():
        logger.debug(f"No significant text content (README/description) found for AI exemption analysis of '{repo_name}'. Skipping AI call.", extra={'org_group': org_group_context_for_log})
        return None, None

    effective_max_input_len =  max_input_tokens_for_combined_text - 500 
    input_text = f"Repository Name: {repo_name}\nDescription: {description}\n\nREADME:\n{readme}"
    if len(input_text) > effective_max_input_len:
        input_text = input_text[:effective_max_input_len] + "\n... [Content Truncated]"
        logger.warning(f"Input text for AI exemption analysis of '{repo_name}' was truncated to fit token limit.", extra={'org_group': org_group_context_for_log})

    prompt = f"""
You are evaluating whether a source code repository should be exempted from code sharing requirements under the SHARE IT Act.
Base your analysis strictly on content and function described in the repository metadata (title, description, README).

Only select an exemption if explicit functional or legal evidence is present in the text.
You may apply one of the following exemption's codes:
{EXEMPT_BY_LAW} - The repository processes or stores legally protected data (e.g., HIPAA, PII, FOIA exclusions, IRB-sensitive datasets).
{EXEMPT_BY_NATIONAL_SECURITY} - Contains elements tied to classified, military, or national security-sensitive content.
{EXEMPT_BY_AGENCY_SYSTEM} - The repository is tightly integrated with CDC-only infrastructure, such as internal IT dashboards, operational monitoring tools, identity systems, or HR-specific logic (e.g., position rating criteria). The code cannot be reused outside CDC without major reconfiguration or poses operational risk if shared.
{EXEMPT_BY_MISSION_SYSTEM} - The repository powers real-time outbreak response, case triage, or operational public health decisions. Releasing the code could impair mission execution, expose vulnerabilities, or cause misinterpretation by external users.
{EXEMPT_BY_CIO} - Appears sensitive or unusually complex but lacks clear evidence; defer to CIO for review (use only if borderline case).

Example1: Title: survey-data-cleaner, README Content: This tool processes raw CDC health surveys containing ZIP codes, birthdates, and patient identifiers before analysis. Data is subject to HIPAA and IRB controls.
Justification Output: {EXEMPT_BY_LAW}|The repository processes HIPAA-regulated health data with personally identifiable information (PII), as stated in the README.
Example2: Title: outbreak-forecast-model, Description: Predicts emerging disease trends using real-time syndromic surveillance inputs.
Output Instructions: README Content: Includes models used by CDC epidemiologists to project infection curves during outbreak scenarios (e.g., flu, COVID-19).
Justification Output:{EXEMPT_BY_MISSION_SYSTEM}|The repository supports outbreak forecasting and is used directly in CDC's public health decision-making.
Example3: Title: internal-logging-dashboard, README Content: Provides metrics aggregation and system logs for internal OCIO-managed infrastructure. Access restricted to CDC internal staff.
Justification Output: {EXEMPT_BY_AGENCY_SYSTEM}|The code supports internal system monitoring for CDC infrastructure and is not intended for public or external use.

If no exemptions clearly apply, output: None
If one or more apply, select one. Return the result as a pair separated by "|" (e.g., EXEMPTION_CODE|JUSTIFICATION)

Do not infer exemptions based on:
-Internal email addresses (@cdc.gov)
-Naming patterns alone (e.g., "nccdphp" or org units)
-General lack of documentation
Repository Information:
    ---
    {input_text}
    ---

    Analysis Result:
    """
    try: # sourcery skip: extract-method
        logger.debug(f"Calling AI model '{cfg_obj.AI_MODEL_NAME_ENV}' for exemption analysis for repository '{repo_name}'...", extra={'org_group': org_group_context_for_log})
        model = genai.GenerativeModel(cfg_obj.AI_MODEL_NAME_ENV)
        response = model.generate_content(
            prompt,
            generation_config=genai.types.GenerationConfig(
                temperature=cfg_obj.AI_TEMPERATURE_ENV,
                max_output_tokens=cfg_obj.AI_MAX_OUTPUT_TOKENS_ENV
            ),
            request_options={"timeout": 30}  # 30 second timeout
        )
        ai_result_text = response.text.strip()
        logger.debug(f"AI raw response for exemption for '{repo_name}': {ai_result_text}", extra={'org_group': org_group_context_for_log})

        if ai_result_text.lower() == "none":
            logger.info(f"AI exemption analysis for '{repo_name}' determined no specific exemption applies.", extra={'org_group': org_group_context_for_log})
            return None, None
        if '|' in ai_result_text:
            parts = ai_result_text.split('|', 1)
            potential_code = parts[0].strip()
            justification = parts[1].strip()
            if potential_code in VALID_AI_EXEMPTION_CODES:
                logger.info(f"AI exemption analysis for '{repo_name}' suggests exemption: {potential_code}. Justification: {justification}", extra={'org_group': org_group_context_for_log})
                return potential_code, f"AI Suggestion: {justification}"
            else:
                logger.warning(f"AI exemption analysis for '{repo_name}' returned an invalid exemption code: '{potential_code}'. Ignoring.", extra={'org_group': org_group_context_for_log})
                return None, None
        else:
            logger.warning(f"AI exemption analysis for '{repo_name}' returned an unexpected format: '{ai_result_text}'. Ignoring.", extra={'org_group': org_group_context_for_log})
            return None, None
    except (google_api_exceptions.InvalidArgument, google_api_exceptions.PermissionDenied) as ai_auth_err:
        err_str = str(ai_auth_err).lower()
        if "api key not valid" in err_str or "api_key_invalid" in err_str or "permission_denied" in err_str:
            logger.error(
                f"{ANSI_RED}Error during AI exemption call for repository '{repo_name}': API key is invalid or lacks permissions. "
                f"Disabling AI for the rest of this run. Error: {ai_auth_err}{ANSI_RESET}"
            , extra={'org_group': org_group_context_for_log})
            _MODULE_AI_ENABLED_STATUS = False # Disable at module level if key is bad
        else:
            logger.error(f"Authorization/Argument error during AI exemption call for '{repo_name}': {ai_auth_err}", extra={'org_group': org_group_context_for_log})
        return None, None
    except (requests.exceptions.SSLError if requests else None, google_api_exceptions.GoogleAPICallError) as ssl_like_error:
        error_message_lower = str(ssl_like_error).lower()
        is_ssl_error = (
            "ssl" in error_message_lower or
            "certificate" in error_message_lower or
            "tlsv1 alert" in error_message_lower or
            "handshake failed" in error_message_lower or
            (isinstance(ssl_like_error, google_api_exceptions.ServiceUnavailable) and "unavailable" in error_message_lower) # ServiceUnavailable can wrap SSL
        )
        if is_ssl_error:
            logger.error(
                f"{ANSI_RED}SSL/Network Error during AI exemption call for '{repo_name}': {ssl_like_error}. "
                f"AI features will be auto-disabled for the rest of this run. "
                f"Please check your corporate network's CA certificate setup in the Docker container (see README).{ANSI_RESET}",
                extra={'org_group': org_group_context_for_log}
            )
            cfg_obj.AI_AUTO_DISABLED_SSL_ERROR = True
        else: # Not an SSL error, but still a service/network issue
            logger.error(f"A non-SSL network/service error occurred during AI exemption call for '{repo_name}': {ssl_like_error}", exc_info=True, extra={'org_group': org_group_context_for_log})
        return None, None
    except Exception as ai_err:
        logger.error(f"Error during AI exemption call for repository '{repo_name}': {ai_err}", extra={'org_group': org_group_context_for_log})
        return None, None
    finally:
        if _MODULE_AI_ENABLED_STATUS and cfg_obj.AI_DELAY_ENABLED_ENV > 0: # Use delay from cfg_obj
            logger.debug(f"Pausing for {cfg_obj.AI_DELAY_ENABLED_ENV} seconds to respect AI rate limit...", extra={'org_group': org_group_context_for_log})
            time.sleep(cfg_obj.AI_DELAY_ENABLED_ENV)

def _extract_emails_from_content(content: Optional[str], source_name: str) -> List[str]:
    if not content: return []
    emails = re.findall(EMAIL_PATTERN, content)
    cdc_emails = [
        email for email in emails if email.lower().endswith("@cdc.gov")
    ]
    return cdc_emails

def _get_combined_contact_emails(repo_data: Dict[str, Any], org_group_context_for_log: str) -> List[str]:
    all_emails = []
    readme_content = repo_data.get('readme_content')
    codeowners_content = repo_data.get('_codeowners_content')
    repo_name_for_log = repo_data.get('name', 'N/A')
    found_contact_line = False

    if readme_content:
        contact_line_matches = CONTACT_LINE_REGEX.finditer(readme_content)
        contact_line_emails = [email for match in contact_line_matches for email in _extract_emails_from_content(match.group(1), f"README 'Contact:' line for {repo_name_for_log}")]
        if contact_line_emails:
            logger.info(f"Prioritizing emails found on 'Contact:' line(s) in README for {repo_name_for_log}.", extra={'org_group': org_group_context_for_log})
            all_emails = contact_line_emails
            found_contact_line = True

    if not found_contact_line:
        codeowners_emails = _extract_emails_from_content(codeowners_content, f"CODEOWNERS for {repo_name_for_log}")
        if codeowners_emails:
            logger.info(f"Prioritizing emails found in CODEOWNERS for {repo_name_for_log} (no 'Contact:' line in README).", extra={'org_group': org_group_context_for_log})
            all_emails = codeowners_emails
        elif readme_content: 
            logger.debug(f"No specific 'Contact:' line in README and no emails in CODEOWNERS for {repo_name_for_log}. Scanning full README.", extra={'org_group': org_group_context_for_log})
            readme_emails = _extract_emails_from_content(readme_content, f"full README for {repo_name_for_log}")
            if readme_emails:
                 logger.info(f"Using emails found in full README scan for {repo_name_for_log} (no 'Contact:' line, no CODEOWNERS emails).", extra={'org_group': org_group_context_for_log})
                 all_emails = readme_emails

    unique_sorted_emails = sorted(list(set(email.lower() for email in all_emails)))
    return unique_sorted_emails

def _strip_html_tags(text: str) -> str:
    return HTML_TAG_REGEX.sub('', text).strip() if text else ""

def _parse_readme_for_version(readme_content: str | None, org_group_context_for_log: str) -> str | None:
    if not readme_content: return None
    match = VERSION_MARKER.search(readme_content)
    if match:
       raw_version_str = match.group(1).strip()
       decoded_version_str = html.unescape(raw_version_str)
       stripped_version_str = _strip_html_tags(decoded_version_str)
       version_str = stripped_version_str.strip('*_`')
       if version_str.lower().startswith('v'):
           version_str = version_str[1:].strip()
       if version_str:
            logger.debug(f"_parse_readme_for_version: Returning cleaned version: '{version_str}'", extra={'org_group': org_group_context_for_log})
            return version_str
    return None

def _parse_readme_for_tags(readme_content: str | None, org_group_context_for_log: str) -> list[str]:
    if not readme_content: return []
    match = TAGS_REGEX.search(readme_content)
    if match:
      tags_line = match.group(1).strip()
      decoded_tags_line = html.unescape(tags_line)
      tags_line_stripped = _strip_html_tags(decoded_tags_line)
      tags = [tag.strip().strip('*_`') for tag in tags_line_stripped.split(',') if tag.strip()]
      logger.debug(f"Found potential tags in README via regex: {tags}", extra={'org_group': org_group_context_for_log})
      return tags
    return []

def _parse_readme_for_status(readme_content: str | None, org_group_context_for_log: str) -> str | None:
    if not readme_content: return None
    match = STATUS_REGEX.search(readme_content)
    if match:
        status_str = match.group(1).strip().lower()
        logger.debug(f"Found potential status in README via regex: '{status_str}'", extra={'org_group': org_group_context_for_log})
        return 'maintained' if status_str == 'active' else status_str
    return None

def _parse_readme_for_labor_hours(readme_content: str | None, org_group_context_for_log: str) -> int | None:
    if not readme_content: return None
    match = LABOR_HOURS_REGEX.search(readme_content)
    if match:
        try:
            return int(match.group(1).strip())
        except (ValueError, IndexError):
            logger.warning(f"Found labor hours pattern in README but failed to parse number: '{match.group(1)}'", extra={'org_group': org_group_context_for_log})
    return None

def _parse_readme_for_organization(readme_content: str | None, repo_name: str, org_group_context_for_log: str) -> str | None:
    if not readme_content: return None
    match = ORGANIZATION_MARKER.search(readme_content)
    if match:
        org_value = match.group(1).strip()
        if org_value:
            org_value = re.sub(r"^(Organization|Org):\s*", "", org_value, flags=re.IGNORECASE).strip()
            org_value = html.unescape(org_value)
            org_value = re.sub(r'<br\s*/?>', ' ', org_value, flags=re.IGNORECASE).strip()
            logger.debug(f"Found and cleaned 'Organization:' marker in README for {repo_name} with value: '{org_value}'", extra={'org_group': org_group_context_for_log})
            return org_value
    return None

def process_repository_exemptions(
    repo_data: Dict[str, Any], 
    scm_org_for_logging: str,
    cfg_obj: Config, 
    default_org_identifiers: Optional[List[str]] = None
) -> Dict[str, Any]: # Assuming 'Any' is a placeholder for 'Config' type
    """
    Processes a repository's data to determine exemptions and set usageType.    
    Returns a dictionary (which could be a modified copy or the original with modifications) 
    containing the processed repository data.
    """
    if not isinstance(repo_data, dict):
        logger.error(f"Invalid repo_data type: {type(repo_data)}. Expected dict.", extra={'org_group': 'ExemptionProcessorInputValidation'})
        return {"name": "ErrorRepo", "processing_error": "Invalid input data type"}
   
    processed_repo_data = repo_data.copy()
    processed_repo_data.setdefault('name', 'UnknownRepo')

    current_permissions = processed_repo_data.setdefault('permissions', {})
    current_permissions.setdefault('usageType', None)
    current_permissions.setdefault('exemptionText', None)
    repo_name = processed_repo_data.get('name', 'UnknownRepo')
    readme_content = processed_repo_data.get('readme_content')
    all_languages = processed_repo_data.get('languages', [])
    is_empty_repo = processed_repo_data.get("_is_empty_repo", False)
    initial_org_from_repo_data = processed_repo_data.get('organization', 'UnknownOrg') 
    # Use the passed-in scm_org_for_logging for the logging context
    org_group_context = scm_org_for_logging

    logger.debug(f"Processing exemptions/fallbacks for SCM org '{scm_org_for_logging}', repo '{repo_name}'. Initial repo_data.organization: '{initial_org_from_repo_data}'.", extra={'org_group': org_group_context})

    if not isinstance(processed_repo_data['permissions'].get('licenses'), list):
        processed_repo_data['permissions']['licenses'] = []

    if not isinstance(processed_repo_data.get('contact'), dict):
        processed_repo_data['contact'] = {}

    is_private_or_internal = processed_repo_data.get('repositoryVisibility', '').lower() in ['private', 'internal']    
    
    is_full_processing_needed = current_permissions.get('usageType') is None

    if not is_full_processing_needed:
        logger.info(
            f"For repo '{repo_name}', using pre-existing/cached usageType: "
            f"'{current_permissions['usageType']}'. Skipping re-evaluation of exemptions, "
            f"organization, and other README-derived fallbacks.",
            extra={'org_group': org_group_context})
        processed_repo_data.setdefault('_is_generic_organization', False)

    pre_existing_emails = processed_repo_data.get('_private_contact_emails')
    actual_contact_emails_for_final_step = [] 

    if '_private_contact_emails' in processed_repo_data and \
        isinstance(pre_existing_emails, list) and \
        pre_existing_emails: 
        logger.info(f"For {repo_name}, using pre-existing _private_contact_emails: {processed_repo_data['_private_contact_emails']}", extra={'org_group': org_group_context})
        actual_contact_emails_for_final_step = pre_existing_emails
    else:
        derived_contact_emails = _get_combined_contact_emails(processed_repo_data, org_group_context)
        processed_repo_data['_private_contact_emails'] = derived_contact_emails
        actual_contact_emails_for_final_step = derived_contact_emails
        logger.info(f"For {repo_name}, contact emails now SET to: {processed_repo_data.get('_private_contact_emails')}", extra={'org_group': org_group_context})

    if is_full_processing_needed:
        logger.info(f"For repo '{repo_name}', no pre-existing usageType. Performing full exemption and data inference.", extra={'org_group': org_group_context})

        should_attempt_ai = (
            cfg_obj.AI_ENABLED_ENV and 
            _MODULE_AI_ENABLED_STATUS and 
            (DISABLE_SSL_ENV != "true") and
            not cfg_obj.AI_AUTO_DISABLED_SSL_ERROR)

        if is_private_or_internal:
                exemption_applied = False
                if readme_content:
                    manual_exempt_match = re.search(r"Exemption:\s*(\S+)", readme_content, re.IGNORECASE | re.MULTILINE)
                    justification_match = re.search(r"Exemption justification:\s*(.*)", readme_content, re.IGNORECASE | re.MULTILINE)
                    if manual_exempt_match and justification_match:
                        captured_code = manual_exempt_match.group(1).strip()
                        if captured_code in VALID_AI_EXEMPTION_CODES or captured_code == EXEMPT_NON_CODE:
                            current_permissions['usageType'] = captured_code
                            current_permissions['exemptionText'] = justification_match.group(1).strip()
                            exemption_applied = True
                            logger.info(f"Repo '{repo_name}': Exempted manually via README ({captured_code}).", extra={'org_group': org_group_context})

                if not exemption_applied:
                    is_purely_non_code = not any(lang and lang.strip().lower() not in [l.lower() for l in NON_CODE_LANGUAGES if l] for lang in all_languages) if all_languages else True
                    if is_purely_non_code:
                        current_permissions['usageType'] = EXEMPT_NON_CODE
                        languages_str = ', '.join(filter(None, all_languages)) or 'None detected'
                        current_permissions['exemptionText'] = f"Non-code repository (languages: [{languages_str}])"
                        exemption_applied = True
                        logger.info(f"Repo '{repo_name}': Exempted as non-code (Languages: [{languages_str}]).", extra={'org_group': org_group_context})

                if not exemption_applied and should_attempt_ai: 
                    if is_empty_repo:
                        logger.info(f"Repository '{repo_name}' is marked as empty. Skipping AI exemption analysis.", extra={'org_group': org_group_context})
                    else:
                        logger.debug(f"Repo '{repo_name}': No standard exemption. Calling AI for exemption analysis.", extra={'org_group': org_group_context})
                        ai_usage_type, ai_exemption_text = _call_ai_for_exemption(
                            repo_data=processed_repo_data,
                            cfg_obj=cfg_obj, # Pass Config object
                            org_group_context_for_log=org_group_context
                        )
                        if ai_usage_type:
                            current_permissions['usageType'] = ai_usage_type
                            current_permissions['exemptionText'] = ai_exemption_text
                            exemption_applied = True
                            logger.info(f"Repo '{repo_name}': Exempted via AI analysis ({ai_usage_type}).", extra={'org_group': org_group_context})

                if not exemption_applied and readme_content:
                    found_keywords = [kw for kw in SENSITIVE_KEYWORDS if re.search(r'\b' + re.escape(kw) + r'\b', readme_content, re.IGNORECASE)]
                    if found_keywords:
                        current_permissions['usageType'] = EXEMPT_BY_LAW
                        current_permissions['exemptionText'] = f"Flagged: Found keywords in README: [{', '.join(found_keywords)}]"
                        exemption_applied = True
                        logger.info(f"Repo '{repo_name}': Exempted due to sensitive keywords ({EXEMPT_BY_LAW}): {found_keywords}.", extra={'org_group': org_group_context})
                
                if not exemption_applied: 
                    if not should_attempt_ai and not is_empty_repo and (DISABLE_SSL_ENV != "true") and not (cfg_obj and cfg_obj.AI_AUTO_DISABLED_SSL_ERROR):
                        logger.debug(f"AI was disabled for exemption analysis for '{repo_name}' (config or module status). Applying default usageType.", extra={'org_group': org_group_context})
                    current_permissions['usageType'] = USAGE_GOVERNMENT_WIDE_REUSE
                    current_permissions['exemptionText'] = None 
        else:  # Public repo
            licenses_list = current_permissions.get('licenses', [])
            has_license = bool(licenses_list)
            current_permissions['usageType'] = USAGE_OPEN_SOURCE if has_license else USAGE_GOVERNMENT_WIDE_REUSE
            current_permissions['exemptionText'] = None
                            
        logger.info(f"For {repo_name}, exemption status in repo_data NOW SET to: usageType='{current_permissions['usageType']}', exemptionText='{current_permissions.get('exemptionText', '(none)')}'", extra={'org_group': org_group_context})

        effective_default_org_ids = list(set(doi.lower() for doi in (default_org_identifiers or []) if doi))
        if initial_org_from_repo_data.lower() not in effective_default_org_ids and \
           initial_org_from_repo_data.lower() not in (val.lower() for val in KNOWN_CDC_ORGANIZATIONS.values()):
            effective_default_org_ids.append(initial_org_from_repo_data.lower())
        if "unknownorg" not in effective_default_org_ids:
            effective_default_org_ids.append("unknownorg")

        prog_org = _programmatic_org_from_repo_name(repo_name, initial_org_from_repo_data, effective_default_org_ids, org_group_context)
        if prog_org:
            processed_repo_data['organization'] = prog_org

        if readme_content:
            extracted_org_from_readme = _parse_readme_for_organization(readme_content, repo_name, org_group_context)
            if extracted_org_from_readme:
                current_org_before_readme = processed_repo_data.get('organization', initial_org_from_repo_data)
                if extracted_org_from_readme.lower() != current_org_before_readme.lower():
                    logger.info(f"Updating organization for '{repo_name}' from README. Previous: '{current_org_before_readme}', README: '{extracted_org_from_readme}'", extra={'org_group': org_group_context})
                    processed_repo_data['organization'] = extracted_org_from_readme

        current_org_after_prog_readme = processed_repo_data.get('organization', 'UnknownOrg').lower()
        if should_attempt_ai:
            if is_empty_repo:
                logger.info(f"Repository '{repo_name}' is marked as empty. Skipping AI organization inference.", extra={'org_group': org_group_context})
            elif current_org_after_prog_readme in effective_default_org_ids:
                ai_org = _call_ai_for_organization(
                    repo_data=processed_repo_data,
                    cfg_obj=cfg_obj, # Pass Config object
                    org_group_context_for_log=org_group_context
                )
                if ai_org and ai_org.lower() != "none":
                    validated_ai_org = next((full_name for acronym, full_name in KNOWN_CDC_ORGANIZATIONS.items() if ai_org.lower() == full_name.lower() or ai_org.lower() == acronym.lower()), None)
                    if validated_ai_org and validated_ai_org.lower() != current_org_after_prog_readme:
                        logger.info(f"Updating organization for '{repo_name}' from AI. Previous: '{processed_repo_data.get('organization', '')}', AI: '{validated_ai_org}'", extra={'org_group': org_group_context})
                        processed_repo_data['organization'] = validated_ai_org
                    elif not validated_ai_org:
                         logger.warning(f"AI suggested org '{ai_org}' for '{repo_name}', but not in known list. Discarding.", extra={'org_group': org_group_context})
            else:
                logger.info(f"Organization for '{repo_name}' is '{processed_repo_data.get('organization', '')}', not calling AI for organization.", extra={'org_group': org_group_context})
        else:
            logger.debug(f"AI is disabled for organization inference for '{repo_name}' (config or module status).", extra={'org_group': org_group_context})

        final_determined_org = processed_repo_data.get('organization', initial_org_from_repo_data)
        is_still_generic_org = False
        if default_org_identifiers and final_determined_org.lower() in [d.lower() for d in default_org_identifiers]:
            is_still_generic_org = True
        elif final_determined_org.lower() == 'unknownorg': 
            is_still_generic_org = True
        processed_repo_data['_is_generic_organization'] = is_still_generic_org

        if readme_content:
            contract_match = re.search(r"^Contract#:\s*(.*)", readme_content, re.MULTILINE | re.IGNORECASE)
            if contract_match:
                processed_repo_data['contractNumber'] = contract_match.group(1).strip()

        if readme_content:
            if processed_repo_data.get("version", "N/A") == "N/A":
                parsed_version = _parse_readme_for_version(readme_content, org_group_context)
                if parsed_version: processed_repo_data["version"] = parsed_version
            if not processed_repo_data.get("tags"): 
                parsed_tags = _parse_readme_for_tags(readme_content, org_group_context)
                if parsed_tags: processed_repo_data["tags"] = parsed_tags
            if processed_repo_data.get("laborHours", 0) == 0:
                parsed_hours = _parse_readme_for_labor_hours(readme_content, org_group_context)
                if parsed_hours is not None and parsed_hours > 0: processed_repo_data["laborHours"] = parsed_hours
            parsed_status = _parse_readme_for_status(readme_content, org_group_context)
            if parsed_status: processed_repo_data["_status_from_readme"] = parsed_status

            licenses = current_permissions.get('licenses', [])
            if licenses and isinstance(licenses, list) and licenses[0] and not licenses[0].get('URL'):
                readme_url = processed_repo_data.get('readme_url')
                if readme_url:
                    potential_license_url = None
                    if 'README.md' in readme_url: potential_license_url = readme_url.replace('README.md', 'LICENSE', 1)
                    elif 'README.txt' in readme_url: potential_license_url = readme_url.replace('README.txt', 'LICENSE', 1)
                    elif '/README' in readme_url:
                        parts = readme_url.split('/')
                        if parts and parts[-1].lower().startswith('readme'):
                            parts[-1] = 'LICENSE'
                            potential_license_url = '/'.join(parts)
                    if potential_license_url and potential_license_url != readme_url:
                        licenses[0]['URL'] = potential_license_url
                        logger.info(f"Repo '{repo_name}': Guessed license URL: {potential_license_url}", extra={'org_group': org_group_context})

    final_json_email = PUBLIC_CONTACT_EMAIL_DEFAULT 
    if is_private_or_internal:
        final_json_email = PRIVATE_CONTACT_EMAIL_DEFAULT
    elif actual_contact_emails_for_final_step: 
        final_json_email = actual_contact_emails_for_final_step[0]
    processed_repo_data['contact']['email'] = final_json_email

    if processed_repo_data.get('contact') and list(processed_repo_data['contact'].keys()) == ['name'] and not processed_repo_data['contact'].get('email'):
        processed_repo_data.pop('contact', None)
    elif processed_repo_data.get('contact') and not processed_repo_data['contact']: 
        processed_repo_data.pop('contact', None)

    processed_repo_data.pop('readme_content', None)
    processed_repo_data.pop('_codeowners_content', None)

    return processed_repo_data
