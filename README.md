# README

This repository contains scripts for processing and transforming Airbnb scraped data and writing it into a MySQL database.

## Scripts

### `main.py`

This script is the main entry point for the data processing pipeline. It performs the following tasks:

1. **Pull Field Actions DataFrame:**
   - Reads a CSV file containing field actions.
   - Applies transformations to the "Field" and "Action" columns.
   - Filters out specific actions.
   - Returns the processed DataFrame.

2. **Get Tables Columns Dictionary:**
   - Takes the processed field actions DataFrame.
   - Creates a dictionary containing tables as keys and associated columns as values.
   - Identifies one-to-many relationships based on the "Processing Action" column.
   - Returns the tables_columns dictionary.

3. **Get Scraped Data DataFrame:**
   - Reads the Airbnb scraped data from a CSV file.
   - Standardizes column names using snake_case and replaces slashes with underscores.
   - Renames the "id_str" column to "guid".
   - Returns the processed DataFrame.

4. **Normalize Data Model:**
   - Takes the tables_columns dictionary and the Airbnb parent DataFrame.
   - Creates child tables based on the specified columns for each table.
   - Returns a dictionary containing child tables.

5. **Pivot One-to-Many Tables:**
   - Takes the dictionary of child tables.
   - Identifies one-to-many relationships and pivots the data accordingly.
   - Returns the updated child tables dictionary.

6. **Write Tables in MySQL:**
   - Takes the child tables dictionary and writes them to a MySQL database.
   - Uses credentials from the `creds.py` file.
   - Blacklists specified tables if provided.

### `utils.py`

This module contains utility functions used in the main script:

- `replace_slashes_by_underscores`: Replaces slashes with underscores in a given string.
- `remove_table_prefix`: Removes the "Table: " prefix from a given string.
- `to_snake_case`: Converts a string to snake_case.
- `extract_first_number`: Extracts the first number from a string.
- `remove_number_between_underscores`: Removes a number between underscores in a string.

## Usage

1. Ensure you have the required dependencies installed (`pandas`, `mysql-connector-python`, `sqlalchemy`).
2. Set up your MySQL database and update the credentials in the `creds.py` file.
3. Place the field actions CSV file in the `input` directory.
4. Place the Airbnb scraped data CSV file in the `input` directory.
5. Run the `main.py` script to execute the data processing pipeline.

Note: Make sure to customize the input and output paths as needed for your specific use case.

## Connecting to DB

mysql -h visithmt.cquajs1oviiz.us-east-2.rds.amazonaws.com -u visithmtadmin