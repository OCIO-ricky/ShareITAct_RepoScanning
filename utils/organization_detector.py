# utils/organization_detector.py
import re
import logging
from collections import Counter
import os

logger = logging.getLogger(__name__)

class OrganizationDetector:
    """Detects organization names from repository metadata."""
    
    # Common organization name patterns in repo names
    ORG_PATTERNS = [
        r'^(?:cdc|CDC)-(.+?)(?:-|$)',  # cdc-orgname-repo or CDC-orgname
        r'^(?:[a-z0-9]+)-([A-Z][A-Za-z]+)(?:-|$)',  # prefix-OrgName-repo
        r'^([A-Z][A-Za-z]+)(?:-|$)',  # OrgName-repo
    ]
    
    # Known organization name mappings (acronyms to full names)
    ORG_MAPPINGS = {
        'cdc': 'Centers for Disease Control and Prevention',
        'ncezid': 'National Center for Emerging and Zoonotic Infectious Diseases',
        'nchhstp': 'National Center for HIV/AIDS, Viral Hepatitis, STD, and TB Prevention',
        'ncird': 'National Center for Immunization and Respiratory Diseases',
        'niosh': 'National Institute for Occupational Safety and Health',
        'ophss': 'Office of Public Health Scientific Services',
        # Add more mappings as needed
    }
    
    # Default organization name if nothing is detected
    DEFAULT_ORG = "Centers for Disease Control and Prevention"
    
    @classmethod
    def detect_from_repo_name(cls, repo_name):
        """Extract organization name from repository name."""
        if not repo_name:
            return None
            
        # Try to match patterns
        for pattern in cls.ORG_PATTERNS:
            match = re.search(pattern, repo_name)
            if match and match.group(1):
                org_part = match.group(1)
                # Check if it's a known acronym
                if org_part.lower() in cls.ORG_MAPPINGS:
                    return cls.ORG_MAPPINGS[org_part.lower()]
                # Convert camelCase or kebab-case to spaces
                org_name = re.sub(r'([a-z])([A-Z])', r'\1 \2', org_part)  # camelCase to spaces
                org_name = org_name.replace('-', ' ')  # kebab-case to spaces
                return org_name
                
        return None
    
    @classmethod
    def detect_from_readme(cls, readme_content):
        """Extract organization name from README content."""
        if not readme_content:
            return None
            
        # Look for common patterns in README files
        patterns = [
            r'(?:developed by|created by|maintained by|©|copyright)\s+(?:the\s+)?([A-Z][A-Za-z\s,&]+?)(?:\.|\n|,|\()',
            r'(?:A|An)\s+([A-Z][A-Za-z\s,&]+?)(?:\s+project|\s+tool|\s+library|\s+application)',
            r'^#\s+([A-Z][A-Za-z\s,&]+?)(?:\n|\s+)',  # Title at the beginning
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, readme_content, re.IGNORECASE)
            if matches:
                # Get the most common match that's not too short
                valid_matches = [m for m in matches if len(m) > 3]
                if valid_matches:
                    return Counter(valid_matches).most_common(1)[0][0].strip()
        
        # Check for known acronyms in the text
        for acronym, full_name in cls.ORG_MAPPINGS.items():
            if re.search(r'\b' + re.escape(acronym) + r'\b', readme_content, re.IGNORECASE):
                return full_name
                
        return None
    
    @classmethod
    def detect_organization(cls, repo_name, repo_path=None, readme_content=None):
        """
        Detect organization name from multiple sources.
        
        Args:
            repo_name (str): Repository name
            repo_path (str, optional): Path to repository
            readme_content (str, optional): README content if already loaded
            
        Returns:
            str: Detected organization name or default
        """
        # Try to detect from repo name
        org_name = cls.detect_from_repo_name(repo_name)
        if org_name:
            logger.debug(f"Detected organization '{org_name}' from repo name '{repo_name}'")
            return org_name
            
        # Try to detect from README if we have content or path
        if not readme_content and repo_path:
            # Look for README files
            readme_paths = [
                os.path.join(repo_path, 'README.md'),
                os.path.join(repo_path, 'README.txt'),
                os.path.join(repo_path, 'README'),
                os.path.join(repo_path, 'readme.md'),
            ]
            
            for path in readme_paths:
                if os.path.exists(path):
                    try:
                        with open(path, 'r', encoding='utf-8') as f:
                            readme_content = f.read()
                        break
                    except Exception as e:
                        logger.warning(f"Error reading README at {path}: {e}")
        
        if readme_content:
            org_name = cls.detect_from_readme(readme_content)
            if org_name:
                logger.debug(f"Detected organization '{org_name}' from README content")
                return org_name
        
        # Return default if nothing detected
        logger.debug(f"Using default organization '{cls.DEFAULT_ORG}' for '{repo_name}'")
        return cls.DEFAULT_ORG