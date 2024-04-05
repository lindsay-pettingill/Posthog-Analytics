import duckdb

con = duckdb.connect(database='posthog_data.duckdb', read_only=False)

con.execute(f"CREATE TABLE IF NOT EXISTS all_event_data AS SELECT *, cast(timestamp as date) as ds, rtrim(STR_SPLIT(elements_chain, '=')[2], 'nth-child') AS link_clicked, cast(geoip_latitude as float) as lat, cast(geoip_longitude as float) as lng FROM event_data")

table_name='all_event_data'

def query_latlong():
  with duckdb.connect(database='posthog_data.duckdb', read_only=False) as con:
      result = con.execute(f"select count(distinct distinct_id) n, lat, lng from {table_name} where len(concat(lat,lng))>7 group by 2,3").fetchdf()
  return result

def query_country():
  with duckdb.connect(database='posthog_data.duckdb', read_only=False) as con:
      result = con.execute(f"select count(distinct distinct_id) n, geoip_country_code from {table_name} group by 2 order by 1 desc limit 5").fetchdf()
  return result

def query_daily_traffic():
  with duckdb.connect(database='posthog_data.duckdb', read_only=False) as con:
      result = con.execute(f"select count(distinct distinct_id) n, ds from {table_name} group by 2 order by ds").fetchdf()
  return result

def query_page_traffic():
  with duckdb.connect(database='posthog_data.duckdb', read_only=False) as con:
      result = con.execute(f"select count(distinct distinct_id) n, ds, pathname from {table_name} group by 2,3 order by ds").fetchdf()
  return result

def query_referring_domain():
  with duckdb.connect(database='posthog_data.duckdb', read_only=False) as con:
      result = con.execute(f"select count(distinct distinct_id) n, ds, referring_domain from {table_name} where referring_domain not like '%replit.dev%' group by 2,3 order by ds").fetchdf()
  return result

def query_links_clicked():
  with duckdb.connect(database='posthog_data.duckdb', read_only=False) as con:
      result = con.execute(f"select count(distinct distinct_id) n, ds, link_clicked from {table_name} where link_clicked not in('null','Articles','Jobs','lindsay pettingill','Investing','@') group by 2,3 order by ds").fetchdf()
  return result