# azure_devops_setup_test.py

print("--- Starting Azure DevOps SDK Setup Test ---")
all_components_ok = True
missing_components = []
imported_successfully = []

# Test 1: BasicAuthentication from msrest.authentication
try:
    print("\nAttempting to import 'BasicAuthentication' from 'msrest.authentication'...")
    from msrest.authentication import BasicAuthentication
    print("  SUCCESS: 'BasicAuthentication' imported successfully.")
    imported_successfully.append("BasicAuthentication from msrest.authentication")
except ImportError as e:
    print(f"  FAILURE: Could not import 'BasicAuthentication'. Error: {e}")
    missing_components.append("BasicAuthentication from msrest.authentication")
    all_components_ok = False

# Test 2: ServicePrincipalCredentials from azure.identity
try:
    print("\nAttempting to import 'ClientSecretCredential' from 'azure.identity' (for Service Principal with secret)...")
    from azure.identity import ClientSecretCredential
    print("  SUCCESS: 'ClientSecretCredential' imported successfully.")
    imported_successfully.append("ClientSecretCredential from azure.identity")
except ImportError as e:
    print(f"  FAILURE: Could not import 'ClientSecretCredential'. Error: {e}")
    missing_components.append("ClientSecretCredential from azure.identity")
    all_components_ok = False

# Test 3: Connection from azure.devops.connection
try:
    print("\nAttempting to import 'Connection' from 'azure.devops.connection'...")
    from azure.devops.connection import Connection
    print("  SUCCESS: 'Connection' imported successfully.")
    imported_successfully.append("Connection from azure.devops.connection")
except ImportError as e:
    print(f"  FAILURE: Could not import 'Connection'. Error: {e}")
    missing_components.append("Connection from azure.devops.connection")
    all_components_ok = False

# Test 4: GitClient from azure.devops.v7_1.git
try:
    print("\nAttempting to import 'GitClient' from 'azure.devops.v7_1.git'...")
    from azure.devops.v7_1.git import GitClient
    print("  SUCCESS: 'GitClient' imported successfully.")
    imported_successfully.append("GitClient from azure.devops.v7_1.git")
except ImportError as e:
    print(f"  FAILURE: Could not import 'GitClient'. Error: {e}")
    missing_components.append("GitClient from azure.devops.v7_1.git")
    all_components_ok = False

# Test 5: CoreClient from azure.devops.v7_1.core
try:
    print("\nAttempting to import 'CoreClient' from 'azure.devops.v7_1.core'...")
    from azure.devops.v7_1.core import CoreClient
    print("  SUCCESS: 'CoreClient' imported successfully.")
    imported_successfully.append("CoreClient from azure.devops.v7_1.core")
except ImportError as e:
    print(f"  FAILURE: Could not import 'CoreClient'. Error: {e}")
    missing_components.append("CoreClient from azure.devops.v7_1.core")
    all_components_ok = False

# Test 6: AzureDevOpsServiceError from azure.devops.exceptions
try:
    print("\nAttempting to import 'AzureDevOpsServiceError' from 'azure.devops.exceptions'...")
    from azure.devops.exceptions import AzureDevOpsServiceError
    print("  SUCCESS: 'AzureDevOpsServiceError' imported successfully.")
    imported_successfully.append("AzureDevOpsServiceError from azure.devops.exceptions")
except ImportError as e:
    print(f"  FAILURE: Could not import 'AzureDevOpsServiceError'. Error: {e}")
    missing_components.append("AzureDevOpsServiceError from azure.devops.exceptions")
    all_components_ok = False


# --- Summary ---
print("\n--- Azure DevOps SDK Setup Test Summary ---")
if all_components_ok:
    print("Overall Status: SUCCESS")
    print("All critical Azure DevOps SDK components were imported successfully:")
    for component in imported_successfully:
        print(f"  - {component}")
else:
    print("Overall Status: FAILURE")
    if imported_successfully:
        print("\nThe following components were imported successfully:")
        for component in imported_successfully:
            print(f"  - {component}")
    if missing_components:
        print("\nThe following critical components could NOT be imported:")
        for component in missing_components:
            print(f"  - {component}")
        print("\nPlease check your Python environment and ensure 'azure-devops' and its dependencies (like 'azure-identity', 'msrest') are correctly installed.")
        print("Refer to the Dockerfile and requirements.txt for installation steps.")

print("--- Test Complete ---")
