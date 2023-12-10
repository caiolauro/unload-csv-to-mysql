import mysql.connector
from creds import db_config
import pandas as pd

schema = "data_assembly"

try:
    # Create a MySQL connection
    connection = mysql.connector.connect(**db_config)

    if connection.is_connected():
        print("Successfully connected to MySQL database!")
        # Show all databases
        # Get a cursor object
        cursor = connection.cursor()
        cursor.execute("SHOW DATABASES")
        databases = cursor.fetchall()
        print("\nDatabases:")
        for database in databases:
            print(database[0])
        # Use a specific database
        cursor.execute(
            f"USE {db_config['database']}"
        )  # replace 'your_database' with the actual database name
        # Show all tables in the selected database
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        print("\nTables:")
        for table_name in tables:
            print(table_name[0])
            # Fetch all data from the table
            cursor.execute(f"SELECT * FROM {table_name};")
            rows = cursor.fetchall()

            # Get column names
            cursor.execute(f"SHOW COLUMNS FROM {table_name}")
            columns = [column[0] for column in cursor.fetchall()]

            # Create a DataFrame for better visualization
            df = pd.DataFrame(rows, columns=columns)

        # Display the data
        print("\nData in the table:")
        print(df)
    else:
        print("Connection to MySQL database failed.")

except mysql.connector.Error as e:
    print(f"Error: {e}")

finally:
    # Close the MySQL connection (if open)
    if "connection" in locals() and connection.is_connected():
        connection.close()
        print("MySQL connection closed.")
