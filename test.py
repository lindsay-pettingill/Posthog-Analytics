import duckdb 

table_name='event_data'
con = duckdb.connect(database='posthog_data.duckdb', read_only=False)

def list_duckdb_tables():
    query = "SELECT table_name FROM information_schema.tables WHERE table_schema='main';"
    tables_df = con.execute(query).fetchall()
    print(tables_df)
  

def list_duckdb_table_columns():
    query = f"SELECT column_name FROM information_schema.columns WHERE table_name='{table_name}'"
    tables_df = con.execute(query).fetchall()
    print(tables_df)

list_duckdb_tables()
list_duckdb_table_columns()

con.close()
