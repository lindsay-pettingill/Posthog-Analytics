import duckdb


# Querying the database
result = con.execute("SELECT * FROM items").fetchall()

# Display the query results
print(result)

# Close the connection when done
con.close()
