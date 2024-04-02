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
            "kind": "HogQLQuery",
            "query": "SELECT * FROM events where toDate(timestamp) = yesterday() LIMIT 10000000"
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
    credentials = service_account.Credentials.from_service_account_info(service_account_info)

    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
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
    properties_df = properties_df.drop(columns=['distinct_id']) 
    properties_df = properties_df.rename(columns=lambda x: x.replace('$', ''))

    return pd.concat([df, properties_df], axis=1).drop(columns=['properties'])


def display_data_with_streamlit(df):
  # Displaying the DataFrame in Streamlit
  st.title('PostHog Analytics Dashboard')
  st.write("Here's our raw data fetched and processed:")
  st.dataframe(df)
  # You can add more interactive widgets here depending on what you want to achieve
  # For example, a simple button to refresh data
  if st.button('Refresh Data'):
      st.rerun()

def fetch_and_process_sheet_data(client):
  spreadsheet = client.open('posthog_analytics_sheet')
  worksheet = spreadsheet.worksheet('analytics_data')
  data = worksheet.get_all_values()
  df = pd.DataFrame(data)
  df.columns = df.iloc[0]
  df = df.iloc[1:]
  df.reset_index(drop=True, inplace=True)
  return process_properties_column(df)
  
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
    
    display_data_with_streamlit(df)

if __name__ == "__main__":
    main()