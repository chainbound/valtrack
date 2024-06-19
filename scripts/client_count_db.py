import sqlite3
from collections import defaultdict
import re

# Connect to the SQLite database
conn = sqlite3.connect('/Users/namangarg/Downloads/validator_tracker.sqlite')

# Create a cursor object
cursor = conn.cursor()

# Query to retrieve the data
query = """
SELECT peer_id, enr, multiaddr, ip, port, last_seen, last_epoch, client_version, possible_validator, max_validator_count, num_observations 
FROM validator_tracker
"""

# Function to extract the client name from the client version string
def get_client_name(client_version):
    match = re.match(r"(\w+)", client_version)
    if match:
        return match.group(1)
    return client_version

# Define the client categories
client_categories = ['prysm', 'lodestar', 'lighthouse', 'nimbus', 'teku', 'grandine', 'erigon', 'other']

# Function to categorize the client name
def categorize_client(client_version):
    client_version_lower = client_version.lower()
    for category in client_categories[:-1]:
        if category in client_version_lower:
            return category
    return 'other'

# Execute the query
cursor.execute(query)

# Fetch all results from the executed query
data = cursor.fetchall()

# Dictionary to store the results
client_data = defaultdict(lambda: {"total_validators": 0, "total_peers_with_validators": 0})

# Process each entry in the fetched data
for entry in data:
    peer_id, enr, multiaddr, ip, port, last_seen, last_epoch, client_version, possible_validator, max_validator_count, num_observations = entry

    client_category = categorize_client(client_version)

    client_data[client_category]["total_validators"] += max_validator_count
    if possible_validator:
        client_data[client_category]["total_peers_with_validators"] += 1

# Output the results
for client_version, counts in client_data.items():
    # if counts["possible_validators"] == 0:
    #         continue
        
    print(f"Client Version: {client_version}")
    print(f"  Total Peers with Validators: {counts['total_peers_with_validators']}")
    print(f"  Total Validator Count: {counts['total_validators']}\n")

# Close the connection
conn.close()