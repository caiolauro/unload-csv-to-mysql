#TBD
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
from creds import user, password

schema = "data_assembly"
# MySQL database connection parameters
db_config = {
'host': 'http://visithmt.cquajs1oviiz.us-east-2.rds.amazonaws.com/',
'user': user,
'password': password,
}


import os
# File path to your CSV files
csv_files = [
'/path/to/your/file1.csv',
'/path/to/your/file2.csv',
]



# # MySQL table name
# table_name = 'your_table_name'



# # Loop through CSV files and load data into MySQL
# for csv_file in csv_files:
# # Read CSV into a pandas DataFrame
# df = pd.read_csv(csv_file)



# # Create a MySQL connection
# connection = mysql.connector.connect(**db_config)



# # Create an SQLAlchemy engine using the MySQL connection
# engine = create_engine(f"mysql+mysqlconnector://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}")



# # Write the pandas DataFrame to the MySQL database
# df.to_sql(table_name, con=engine, if_exists='append', index=False)



# # Close the MySQL connection
# connection.close()



# print("Data loaded successfully.")