import pandas as pd

def read_and_print_parquet(file_path):
    try:
        # Read the Parquet file
        df = pd.read_parquet(file_path)
        
        # Print the contents of the DataFrame
        print(df)
        
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    file_path = '/Users/namangarg/code/valtrack/discovery_events.parquet'
    read_and_print_parquet(file_path)
