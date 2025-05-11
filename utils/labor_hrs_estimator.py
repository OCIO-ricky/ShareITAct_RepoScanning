# utils/labor_hrs_estimator.py
# These functions are designed to estimate labor hours based on repository commit history.  
# It is being called within each connector's fetch_repositories() function.
import os
import subprocess
import requests
from datetime import datetime
import pandas as pd
import logging
import base64
import re # For parsing Link header
from typing import Optional, Dict, Any, List, Tuple

# It's good practice to have a logger instance per module
logger = logging.getLogger(__name__)

# --- Helper Functions ---
def _create_summary_dataframe(commit_records: List[Tuple[str, str, datetime]], hours_per_commit: float) -> pd.DataFrame:
    """
    Helper function to create a summary DataFrame from commit records.
    Each record in commit_records should be a tuple: (author_name, author_email, commit_date_datetime).
    """
    df_columns = ["Author", "Email", "Commits", "FirstCommit", "LastCommit", "EstimatedHours"]
    if not commit_records:
        logger.debug("No commit records provided; returning empty DataFrame.")
        return pd.DataFrame(columns=df_columns)

    try:
        df = pd.DataFrame(commit_records, columns=["Author", "Email", "Date"])
        if df.empty:
            return pd.DataFrame(columns=df_columns)

        df["EstimatedHours"] = hours_per_commit
        
        summary_df = df.groupby(["Author", "Email"], as_index=False).agg(
            Commits=("Date", "count"),
            FirstCommit=("Date", "min"),
            LastCommit=("Date", "max"),
            EstimatedHours=("EstimatedHours", "sum")
        )
        return summary_df
    except Exception as e:
        logger.error(f"Error creating summary DataFrame: {e}", exc_info=True)
        return pd.DataFrame(columns=df_columns)

def _get_azure_devops_auth_header_val(pat_token: str) -> Optional[str]:
    """Creates the Basic Authentication header value for Azure DevOps PAT."""
    if not pat_token:
        logger.error("Azure DevOps PAT token cannot be empty for Basic Authentication.")
        return None
    try:
        # The PAT itself is used as the password with an empty username for Basic Auth
        return "Basic " + base64.b64encode(f":{pat_token}".encode()).decode()
    except Exception as e:
        logger.error(f"Failed to encode Azure DevOps PAT: {e}", exc_info=True)
        return None

def _parse_github_link_header(link_header: Optional[str]) -> Dict[str, str]:
    """Parses the GitHub Link header to find the 'next' page URL."""
    links = {}
    if link_header:
        parts = link_header.split(',')
        for part in parts:
            match = re.match(r'<(.*?)>; rel="(.*?)"', part.strip())
            if match:
                links[match.group(2)] = match.group(1)
    return links

# --- Analysis Functions ---

def analyze_local_git(repo_path: str, hours_per_commit: float = 0.5) -> pd.DataFrame:
    """Estimate labor hours from a local Git repo."""
    logger.info(f"Analyzing local Git repository at: {repo_path}")
    log_format = "--pretty=format:%H|%an|%ae|%ad" # Hash|AuthorName|AuthorEmail|AuthorDate(iso)
    records: List[Tuple[str, str, datetime]] = []

    try:
        result = subprocess.run(
            ["git", "log", log_format, "--date=iso"],
            stdout=subprocess.PIPE,
            text=True,
            check=True,
            cwd=repo_path,
            encoding='utf-8'
        )
        log_data = result.stdout.strip().split('\n')

        for line in log_data:
            if not line.strip(): continue
            parts = line.split('|')
            if len(parts) == 4:
                _hash, author, email, date_str = parts
                try:
                    records.append((author.strip(), email.strip(), datetime.fromisoformat(date_str.strip())))
                except ValueError as ve:
                    logger.warning(f"Could not parse date '{date_str.strip()}' for commit by {author}. Skipping. Error: {ve}")
            else:
                logger.warning(f"Malformed git log line in {repo_path}: '{line}'. Skipping.")
    
    except FileNotFoundError:
        logger.error(f"Git command not found or '{repo_path}' is not a valid directory. Ensure Git is installed and in PATH.")
    except subprocess.CalledProcessError as e:
        logger.error(f"Error running 'git log' in '{repo_path}': {e.stderr or e.stdout or e}")
    except Exception as e:
        logger.error(f"Unexpected error analyzing local git repo '{repo_path}': {e}", exc_info=True)

    if not records:
        logger.info(f"No commit records found or parsed for local repo: {repo_path}")
    return _create_summary_dataframe(records, hours_per_commit)


def analyze_github_repo_sync(
    owner: str, 
    repo: str, 
    token: str, 
    hours_per_commit: float = 0.5, 
    github_api_url: str = "https://api.github.com",
    session: Optional[requests.Session] = None
) -> pd.DataFrame:
    """Estimate labor hours from a GitHub repo using its API (synchronously)."""
    logger.info(f"Analyzing GitHub repository: {owner}/{repo} using API: {github_api_url}")
    
    _session_managed_internally = False
    if session is None:
        session = requests.Session()
        _session_managed_internally = True
        session.headers.update({
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github.v3+json"
        })
        logger.debug("No external session provided; created and configured internal session for GitHub.")
    else:
        logger.debug("Using externally provided session for GitHub.")

    all_commits_data: List[Dict[str, Any]] = []
    next_page_url: Optional[str] = f"{github_api_url.rstrip('/')}/repos/{owner}/{repo}/commits?per_page=100"
    processed_commits: List[Tuple[str, str, datetime]] = []

    try:
        while next_page_url:
            try:
                logger.debug(f"Fetching GitHub commits from: {next_page_url}")
                res = session.get(next_page_url, timeout=30)
                res.raise_for_status()
                
                page_data = res.json()
                if not page_data: break
                
                all_commits_data.extend(page_data)
                
                link_header = res.headers.get("Link")
                links = _parse_github_link_header(link_header)
                next_page_url = links.get("next")

            except requests.exceptions.HTTPError as e:
                logger.error(f"HTTP error fetching GitHub commits for {owner}/{repo} from {next_page_url}: {e.response.status_code} - {e.response.text}")
                break 
            except requests.exceptions.RequestException as e:
                logger.error(f"Request error fetching GitHub commits for {owner}/{repo} from {next_page_url}: {e}", exc_info=True)
                break
            except ValueError as e: # JSONDecodeError
                logger.error(f"Error decoding JSON from GitHub for {owner}/{repo} from {next_page_url}: {e}", exc_info=True)
                break
        
        for commit_item in all_commits_data:
            commit_details = commit_item.get("commit")
            if commit_details:
                author_info = commit_details.get("author", {})
                author_name = author_info.get("name", "Unknown Author")
                author_email = author_info.get("email", "unknown@example.com")
                date_str = author_info.get("date")
                if date_str:
                    try:
                        commit_date = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                        processed_commits.append((author_name, author_email, commit_date))
                    except ValueError as ve:
                        logger.warning(f"Could not parse date '{date_str}' for GitHub commit. Error: {ve}")
    finally:
        if _session_managed_internally and session:
            session.close()
            logger.debug("Closed internally managed session for GitHub.")
            
    if not processed_commits:
        logger.info(f"No commit records found or parsed for GitHub repo: {owner}/{repo}")
    return _create_summary_dataframe(processed_commits, hours_per_commit)


def analyze_gitlab_repo_sync(
    project_id: str, 
    token: str, 
    hours_per_commit: float = 0.5, 
    gitlab_api_url: str = "https://gitlab.com",
    session: Optional[requests.Session] = None
) -> pd.DataFrame:
    """Estimate labor hours from a GitLab repo using its API (synchronously)."""
    logger.info(f"Analyzing GitLab project ID: {project_id} using API: {gitlab_api_url}")

    _session_managed_internally = False
    if session is None:
        session = requests.Session()
        _session_managed_internally = True
        session.headers.update({"PRIVATE-TOKEN": token})
        logger.debug("No external session provided; created and configured internal session for GitLab.")
    else:
        logger.debug("Using externally provided session for GitLab.")

    all_commits_data: List[Dict[str, Any]] = []
    page = 1
    per_page = 100
    processed_commits: List[Tuple[str, str, datetime]] = []

    try:
        while True:
            url = f"{gitlab_api_url.rstrip('/')}/api/v4/projects/{project_id}/repository/commits?page={page}&per_page={per_page}"
            try:
                logger.debug(f"Fetching GitLab commits from: {url}")
                res = session.get(url, timeout=30)
                res.raise_for_status()
                
                page_data = res.json()
                if not page_data: break
                
                all_commits_data.extend(page_data)
                
                next_page_header = res.headers.get("X-Next-Page")
                if next_page_header and next_page_header.strip():
                    page = int(next_page_header)
                elif len(page_data) < per_page:
                    break
                else: # Should have X-Next-Page if more items exist and we got a full page
                    logger.debug("No X-Next-Page header but full page returned, assuming last page for GitLab.")
                    break

            except requests.exceptions.HTTPError as e:
                logger.error(f"HTTP error fetching GitLab commits for project ID {project_id} from {url}: {e.response.status_code} - {e.response.text}")
                break
            except requests.exceptions.RequestException as e:
                logger.error(f"Request error fetching GitLab commits for project ID {project_id} from {url}: {e}", exc_info=True)
                break
            except ValueError as e: # JSONDecodeError
                logger.error(f"Error decoding JSON from GitLab for project ID {project_id} from {url}: {e}", exc_info=True)
                break
        
        for commit_item in all_commits_data:
            author_name = commit_item.get("author_name", "Unknown Author")
            author_email = commit_item.get("author_email", "unknown@example.com")
            date_str = commit_item.get("created_at")
            if date_str:
                try:
                    commit_date = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                    processed_commits.append((author_name, author_email, commit_date))
                except ValueError as ve:
                    logger.warning(f"Could not parse date '{date_str}' for GitLab commit. Error: {ve}")
    finally:
        if _session_managed_internally and session:
            session.close()
            logger.debug("Closed internally managed session for GitLab.")

    if not processed_commits:
        logger.info(f"No commit records found or parsed for GitLab project ID: {project_id}")
    return _create_summary_dataframe(processed_commits, hours_per_commit)


def analyze_azure_devops_repo_sync(
    organization: str, 
    project: str, 
    repo_id: str, 
    pat_token: str, # Expects the raw PAT
    hours_per_commit: float = 0.5,
    azure_devops_api_url: str = "https://dev.azure.com",
    session: Optional[requests.Session] = None
) -> pd.DataFrame:
    """Estimate labor hours from Azure DevOps repo using its API (synchronously)."""
    logger.info(f"Analyzing Azure DevOps repository: {organization}/{project}/{repo_id} using API: {azure_devops_api_url}")
    
    _session_managed_internally = False
    if session is None:
        auth_header_val = _get_azure_devops_auth_header_val(pat_token)
        if not auth_header_val:
            logger.error("Failed to generate Azure DevOps auth header for internal session.")
            return _create_summary_dataframe([], hours_per_commit)
        
        session = requests.Session()
        _session_managed_internally = True
        session.headers.update({"Authorization": auth_header_val})
        logger.debug("No external session provided; created and configured internal session for Azure DevOps.")
    else:
        logger.debug("Using externally provided session for Azure DevOps.")

    all_commits_data: List[Dict[str, Any]] = []
    skip = 0
    top = 100
    api_version = "6.0"
    processed_commits: List[Tuple[str, str, datetime]] = []

    try:
        while True:
            url = (f"{azure_devops_api_url.rstrip('/')}/{organization}/{project}/_apis/git/repositories/{repo_id}"
                   f"/commits?api-version={api_version}&$top={top}&$skip={skip}")
            try:
                logger.debug(f"Fetching Azure DevOps commits from: {url}")
                res = session.get(url, timeout=30)
                res.raise_for_status()
                
                data = res.json()
                page_commits = data.get("value", [])
                
                if not page_commits: break
                all_commits_data.extend(page_commits)
                if len(page_commits) < top: break
                skip += top

            except requests.exceptions.HTTPError as e:
                logger.error(f"HTTP error fetching Azure DevOps commits for {repo_id} from {url}: {e.response.status_code} - {e.response.text}")
                break
            except requests.exceptions.RequestException as e:
                logger.error(f"Request error fetching Azure DevOps commits for {repo_id} from {url}: {e}", exc_info=True)
                break
            except ValueError as e: # JSONDecodeError
                logger.error(f"Error decoding JSON from Azure DevOps for {repo_id} from {url}: {e}", exc_info=True)
                break
        
        for commit_item in all_commits_data:
            author_info = commit_item.get("author", {})
            author_name = author_info.get("name", "Unknown Author")
            author_email = author_info.get("email", "unknown@example.com")
            date_str = author_info.get("date")
            if date_str:
                try:
                    commit_date = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                    processed_commits.append((author_name, author_email, commit_date))
                except ValueError as ve:
                    logger.warning(f"Could not parse date '{date_str}' for Azure DevOps commit. Error: {ve}")
    finally:
        if _session_managed_internally and session:
            session.close()
            logger.debug("Closed internally managed session for Azure DevOps.")
            
    if not processed_commits:
        logger.info(f"No commit records found or parsed for Azure DevOps repo: {organization}/{project}/{repo_id}")
    return _create_summary_dataframe(processed_commits, hours_per_commit)


