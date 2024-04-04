import duckdb

table_name='event_data'
con = duckdb.connect(database='posthog_data.duckdb', read_only=False)

con.execute(f"CREATE TEMPORARY TABLE all_event_data AS SELECT *, cast(timestamp as date) as ds, rtrim(STR_SPLIT(elements_chain, '=')[2], 'nth-child') AS link_clicked, cast(geoip_latitude as float) as lat, cast(geoip_longitude as float) as lng FROM {table_name}")

def query_latlong():
  result = con.execute("select count(distinct distinct_id) n, lat, lng from all_event_data where len(concat(lat,lng))>7 group by 2,3").fetchdf()
  return result

def query_country():
  result = con.execute("select count(distinct distinct_id) n, geoip_country_code from all_event_data group by 2 order by 1 desc limit 5").fetchdf()
  return result

def query_daily_traffic():
  result = con.execute("select count(distinct distinct_id) n, ds from all_event_data group by 2 order by ds").fetchdf()
  return result



