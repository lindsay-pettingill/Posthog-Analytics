import duckdb

def query_all_data():
  con = duckdb.connect(database='all_data.duckdb', read_only=False)
  result = con.execute("SELECT * FROM event_data limit 1").fetchall()
  con.close()
  return result

## add queries for various plots here, and then import into main.py 