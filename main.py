import requests
import pandas as pd
import gspread
from google.oauth2 import service_account
import json
import os
import streamlit as st
import duckdb

# Constants
HOGQL_API_KEY = os.environ['hogql_api']
GOOGLE_SHEETS_API = os.environ['google_sheets']
PROJECT_ID = os.environ['project_id']

def fetch_data_from_posthog():
  headers = {
      "Content-Type": "application/json",
      "Authorization": f"Bearer {HOGQL_API_KEY}",
  }

  data = {
      "query": {
          "kind":
          "HogQLQuery",
          "query":
          "SELECT * FROM events where toDate(timestamp) = yesterday() LIMIT 10000000"
      }
  }

  response = requests.post(
      f"https://app.posthog.com/api/projects/{PROJECT_ID}/query",
      headers=headers,
      json=data,
  )

  if response.status_code != 200:
    print("Failed to fetch data:", response.status_code, response.text)
    return None

  return response.json()


def initialize_google_sheets():
  service_account_info = json.loads(GOOGLE_SHEETS_API)
  credentials = service_account.Credentials.from_service_account_info(
      service_account_info)

  scope = [
      'https://spreadsheets.google.com/feeds',
      'https://www.googleapis.com/auth/drive'
  ]
  creds = credentials.with_scopes(scope)
  client = gspread.authorize(creds)

  return client


def append_data_to_worksheet(client, df):
  spreadsheet = client.open('posthog_analytics_sheet')
  worksheet = spreadsheet.worksheet('analytics_data')
  df_values = df.values.tolist()
  worksheet.append_rows(df_values)


def process_properties_column(df):

  def safe_json_loads(s):
    try:
      return json.loads(s) if not isinstance(s, dict) else s
    except ValueError:
      return {}

  properties_dicts = df['properties'].apply(safe_json_loads)
  properties_df = pd.json_normalize(properties_dicts)
  properties_df = properties_df.drop(columns=['distinct_id', 'session_id', 'window_id'])
  properties_df = properties_df.rename(columns=lambda x: x.replace('$', ''))

  return pd.concat([df, properties_df], axis=1).drop(columns=['properties'])


def fetch_and_process_sheet_data(client):
  spreadsheet = client.open('posthog_analytics_sheet')
  worksheet = spreadsheet.worksheet('analytics_data')
  data = worksheet.get_all_values()
  df = pd.DataFrame(data)
  df.columns = df.iloc[0]
  df = df.iloc[1:]
  df.columns = [col.replace('$', '') for col in df.columns]
  df.columns = [col.replace('.', '') for col in df.columns]
  df = df.map(lambda x: x.replace('$', ''))
  df = df.loc[:,~df.columns.duplicated()] 
  df.reset_index(drop=True, inplace=True)
  return process_properties_column(df)


  # Function to map pandas data types to SQL data types
def generate_create_table_statement(df, table_name='event_data'):
  sanitized_columns = df.columns.str.replace('.', '__', regex=False)
  df.columns = sanitized_columns
  # Function to map pandas data types to SQL data types
  def pandas_type_to_sql(pandas_type):
    if pandas_type == 'object':
      return 'VARCHAR'
    elif 'int' in str(pandas_type):
      return 'INT'
    elif 'float' in str(pandas_type):
      return 'FLOAT'
    elif pandas_type == 'bool':
      return 'BOOLEAN'
    elif 'datetime' in str(pandas_type):
      return 'TIMESTAMP'
    else:
      return 'VARCHAR'  # Default type

  # Constructing the CREATE TABLE statement
  column_definitions = ', '.join([
      f"{col} {pandas_type_to_sql(dtype)}" for col, dtype in df.dtypes.items()
  ])
  create_table_stmt = f"CREATE TABLE {table_name} ({column_definitions});"
  return create_table_stmt

def data_to_duckdb(df, table_name='event_data'): 
  con = duckdb.connect(database='posthog_data.duckdb', read_only=False)

  drop_table_query = f"DROP TABLE IF EXISTS {table_name};"
  con.execute(drop_table_query)

  # Execute SQL queries
  create_table_statement = generate_create_table_statement(df)
  con.execute(create_table_statement)
  # Preparing and executing the INSERT statements from DataFrame
  placeholders = ", ".join(["?" for _ in range(len(df.columns))])
  insert_query = f"INSERT INTO {table_name} VALUES ({placeholders})"
  for row in df.itertuples(index=False, name=None):
    con.execute(insert_query, list(row))

  # Fetching all data to verify insertion
  all_data = con.execute(f"SELECT * FROM {table_name}").fetchall()

def display_data_with_streamlit(df):
  from queries import query_latlong, query_country, query_daily_traffic
  # Remove or debug duplicate columns
  df = df.loc[:,~df.columns.duplicated()]
  # Or for debugging what the duplicates are:
  duplicate_cols = df.columns[df.columns.duplicated()].unique()
  #print(f"Duplicate Columns (if any): {duplicate_cols}")
  st.title('PostHog Analytics Dashboard')

  lat_long_data = query_latlong()
  lat_long_data_df = pd.DataFrame(lat_long_data, columns=['n', 'lat', 'lng'])
  st.title('Map of Visitors')
  st.map(lat_long_data_df,latitude='lat', longitude='lng', color=None, size='n')

  country_data = query_country()
  country_data_df = pd.DataFrame(country_data, columns=['n', 'geoip_country_code'])
  st.title('Traffic by Country: Top 5')
  st.dataframe(country_data_df)

  daily_traffic = query_daily_traffic()
  daily_traffic_df = pd.DataFrame(daily_traffic, columns=['n', 'ds'])
  st.title('Daily Traffic')
  st.line_chart(daily_traffic_df, x='ds', y='n')

  st.write("Here's our event data, fetched and processed:")
  st.dataframe(df)
  # You can add more interactive widgets here depending on what you want to achieve
  if st.button('Refresh Data'):
    st.rerun()
    
def main():
  response_data = fetch_data_from_posthog()
  if not response_data:
    return

  results = response_data['results']
  columns = response_data['columns']
  df = pd.DataFrame(results, columns=columns)

  client = initialize_google_sheets()

  append_data_to_worksheet(client, df)

  df = fetch_and_process_sheet_data(client)

  generate_create_table_statement(df)

# Insert the processed data into the DuckDB database
  data_to_duckdb(df)

  display_data_with_streamlit(df)
  
if __name__ == "__main__":
  main()
