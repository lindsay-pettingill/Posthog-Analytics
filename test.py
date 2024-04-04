import duckdb 

def list_duckdb_tables():
    con = duckdb.connect(database='all_event_data.duckdb', read_only=False)
    query = "SELECT table_name FROM information_schema.tables WHERE table_schema='main';"
    tables_df = con.execute(query).fetchdf()
    print(tables_df)
list_duckdb_tables()