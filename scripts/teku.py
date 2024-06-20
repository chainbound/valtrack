import re

def parse_logs(log_file_path):
    teku_peers = {}
    handshake_errors = {}
    goodbye_messages = {}

    # Regular expressions to match relevant log lines
    teku_client_pattern = re.compile(r'client_version=teku/[^ ]+ peer=([a-zA-Z0-9]+)')
    handshake_error_pattern = re.compile(r'Handshake failed.*error="([^"]+)".*peer=([a-zA-Z0-9]+)')
    goodbye_message_pattern = re.compile(r'Received goodbye message\s+msg="([^"]+)".*peer=([a-zA-Z0-9]+)')


    with open(log_file_path, 'r') as log_file:
        for line in log_file:
            # Check for teku client version lines
            teku_match = teku_client_pattern.search(line)
            if teku_match:
                peer_id = teku_match.group(1)
                if peer_id not in teku_peers:
                    teku_peers[peer_id] = line.strip()

            # Check for handshake failed lines
            handshake_error_match = handshake_error_pattern.search(line)
            if handshake_error_match:
                error_reason = handshake_error_match.group(1)
                peer_id = handshake_error_match.group(2)
                if peer_id in teku_peers and peer_id not in handshake_errors:
                    handshake_errors[peer_id] = error_reason
            
            # Check for goodbye message lines
            goodbye_message_match = goodbye_message_pattern.search(line)
            if goodbye_message_match:
                goodbye_msg = goodbye_message_match.group(1)
                peer_id = goodbye_message_match.group(2)
                if peer_id in teku_peers:
                    goodbye_messages[peer_id] = goodbye_msg
                
            

    # Print handshake errors for teku peers
    print(f"Teku Peers with handshake errors ({len(handshake_errors)}):")
    for peer_id, error_reason in handshake_errors.items():
        print(f"Peer ID: {peer_id}, Error: {error_reason}")

    # Print goodbye messages for teku peers
    print(f"\n Teku Peers with goodbye messages ({len(goodbye_messages)}):")
    for peer_id, goodbye_msg in goodbye_messages.items():
        print(f"Peer ID: {peer_id}, Sent Goodbye Message: {goodbye_msg}")
            
    
    # Print peers present in both handshake errors and goodbye messages
    common_peers = set(handshake_errors.keys()) & set(goodbye_messages.keys())
    if common_peers:
        print(f"\n Teku Peers with both the goodbye and handshake failed messages ({len(common_peers)}):")
        for peer_id in common_peers:
            print(f"Peer ID: {peer_id}, Sent Goodbye Message and Handshake Failed")
            
    

if __name__ == "__main__":
    log_file_path = 'sentry.log'  # Replace with the path to your log file
    parse_logs(log_file_path)
