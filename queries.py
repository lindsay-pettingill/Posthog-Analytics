import duckdb
from main import generate_create_table_statement
import pandas as pd

con = duckdb.connect(database=':memory:', read_only=False)

# Generate the CREATE TABLE statement from the DataFrame structure using the imported function
create_table_statement = generate_create_table_statement(df)

# Execute CREATE TABLE SQL query
con.execute(create_table_statement)

# Preparing values for insertion (This may differ based on your actual data)
values = ', '.join([f"({row.id}, '{row.name}', {row.price})" for index, row in df.iterrows()])

# Execute INSERT INTO SQL query
con.execute(f"INSERT INTO analytics_data VALUES {values}")

# Querying the database
result = con.execute("SELECT * FROM analytics_data").fetchall()

# Display the query results
print(result)

# Close the connection when done
con.close()