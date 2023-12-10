import os
import pandas as pd


def merge_csv_files(directory_path):
    # Get a list of all CSV files in the specified directory
    csv_files = [file for file in os.listdir(directory_path) if file.endswith(".csv")]

    # Check if there are any CSV files in the directory
    if not csv_files:
        print("No CSV files found in the directory.")
        return

    # Initialize an empty DataFrame to store the merged data
    merged_data = pd.DataFrame()

    # Iterate through each CSV file and merge it into the main DataFrame
    for csv_file in csv_files:
        file_path = os.path.join(directory_path, csv_file)
        try:
            # Read the CSV file into a DataFrame
            df = pd.read_csv(file_path)
            # Merge the current DataFrame with the main DataFrame
            merged_data = pd.concat([merged_data, df], ignore_index=True)
            print(f"Merged {csv_file}")
        except Exception as e:
            print(f"Error reading {csv_file}: {e}")

    # Save the merged DataFrame to a new CSV file
    merged_data.to_csv(
        os.path.join(output_directory_path, "merged_data.csv"), index=False
    )
    print("Merged data saved to merged_data.csv")


# Specify the directory path where the CSV files are located
input_directory_path = "input"
output_directory_path = "output"

# Call the function to merge the CSV files
merge_csv_files(input_directory_path)
