import requests
from collections import defaultdict

# API endpoint
url = "http://localhost:8080/validators"

# Make the API request
response = requests.get(url)

# Check if the request was successful
if response.status_code == 200:
    # Parse the JSON response
    data = response.json()

    # Dictionary to store the results
    client_data = defaultdict(lambda: {"total_validators": 0, "possible_validators": 0})

    # Process each entry in the response
    for entry in data:
        client_version = entry["client_version"]
        max_validator_count = entry["max_validator_count"]
        possible_validator = entry["possible_validator"]

        client_data[client_version]["total_validators"] += max_validator_count
        if possible_validator:
            client_data[client_version]["possible_validators"] += 1

    # Output the results
    for client_version, counts in client_data.items():
        if counts["possible_validators"] == 0:
            continue
        
        print(f"Client Version: {client_version}")
        print(f"  Total Validator Count: {counts['total_validators']}")
        print(f"  Total Peers with Validators: {counts['possible_validators']}\n")

else:
    print(f"Failed to retrieve data from API. Status code: {response.status_code}")
