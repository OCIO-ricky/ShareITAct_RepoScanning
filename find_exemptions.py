import json
import os

def find_exempted_repos(json_filepath="output/code.json"):
    """
    Loads the code.json file and prints details of exempted repositories.

    Args:
        json_filepath (str): The path to the code.json file.

    Returns:
        list: A list of exempted repository dictionaries, or None if file not found.
    """
    exempted_list = []
    if not os.path.exists(json_filepath):
        print(f"Error: File not found at {json_filepath}")
        return None

    try:
        with open(json_filepath, 'r', encoding='utf-8') as f:
            all_repos_data = json.load(f)

        print(f"Searching for exempted repositories in {json_filepath}...")

        for repo_data in all_repos_data:
            # Check if the 'exempted' key exists and is True
            if repo_data.get('exempted', False): # Use .get() for safety
                exempted_list.append(repo_data)
                # Print some details immediately
                print(f"  - Found Exempted Repo:")
                print(f"    Name: {repo_data.get('repo_name', 'N/A')}")
                print(f"    Org:  {repo_data.get('org_name', 'N/A')}")
                print(f"    ID:   {repo_data.get('privateID', 'N/A')}")
                print(f"    Reason: {repo_data.get('exemption_reason', 'Not specified')}")
                print("-" * 20)

        if not exempted_list:
            print("No exempted repositories found.")
        else:
            print(f"\nTotal exempted repositories found: {len(exempted_list)}")

        return exempted_list

    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from {json_filepath}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None

# --- Run the search ---
if __name__ == "__main__":
    find_exempted_repos() # Searches for output/code.json by default
