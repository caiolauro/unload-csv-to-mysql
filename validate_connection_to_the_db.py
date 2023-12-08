import mysql.connector
from creds import db_config

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
        cursor.execute(f"USE {db_config['database']}")  # replace 'your_database' with the actual database name
        # Show all tables in the selected database
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        print("\nTables:")
        for table in tables:
            print(table[0])
    else:
        print("Connection to MySQL database failed.")

except mysql.connector.Error as e:
    print(f"Error: {e}")

finally:
    # Close the MySQL connection (if open)
    if 'connection' in locals() and connection.is_connected():
        connection.close()
        print("MySQL connection closed.")
