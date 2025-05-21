import requests
import configparser
import logging
import os
from requests.exceptions import HTTPError

log_dir = "/app/logs"
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    filename=os.path.join(log_dir, "metabase_setup.log"),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

config = configparser.ConfigParser()
config.read('/app/config/metabase_config.ini')

METABASE_URL = config['metabase']['url'].rstrip('/')
USERNAME = config['metabase']['user']
PASSWORD = config['metabase']['password']

def login():
    res = requests.post(f"{METABASE_URL}/api/session", json={
        "username": USERNAME,
        "password": PASSWORD
    })
    res.raise_for_status()
    logging.info("Authenticated with Metabase.")
    session_id = res.json()['id']
    logging.info(f"Session ID: {session_id}")
    return session_id

def get_database_and_table(session, table_name):
    headers = {"X-Metabase-Session": session}
    dbs = requests.get(f"{METABASE_URL}/api/database", headers=headers).json()
    db = next((db for db in dbs['data'] if db['engine'] == 'snowflake'), None)
    if not db:
        raise Exception("Snowflake database not found.")

    db_id = db['id']
    tables = requests.get(f"{METABASE_URL}/api/table", headers=headers).json()
    table = next((t for t in tables if t['name'].lower() == table_name.lower()), None)
    if not table:
        raise Exception(f"{table_name} table not found.")

    logging.info(f"Found table {table_name} with ID: {table['id']}")
    return db_id, table['id']

def get_fields(table_id, session):
    headers = {"X-Metabase-Session": session}
    try:
        res = requests.get(f"{METABASE_URL}/api/table/{table_id}/query_metadata", headers=headers)
        res.raise_for_status()
        fields = {f['name'].upper(): f['id'] for f in res.json()['fields']}
        logging.info(f"Available fields for table {table_id}: {fields}")
        return fields
    except HTTPError as e:
        logging.error(f"Failed to get fields for table {table_id}: {e}, Response: {res.text}")
        raise

def create_question(session, db_id, table_name, query_config):
    headers = {"X-Metabase-Session": session}
    _, table_id = get_database_and_table(session, table_name)
    fields = get_fields(table_id, session)

    for col in query_config["columns"]:
        if col not in fields:
            raise Exception(f"Column {col} not found in table {table_name}. Available fields: {fields}")

    # Set up bar chart visualization
    visualization_settings = {
        "graph.dimensions": [query_config["x_axis"]],  # X-axis (category)
        "graph.metrics": [query_config["y_axis"]],     # Y-axis (value)
        "graph.x_axis.title_text": query_config["x_axis"],
        "graph.y_axis.title_text": query_config["y_axis"],
        "graph.show_values": True  # Show values on bars
    }

    payload = {
        "name": query_config["name"],
        "dataset_query": {
            "type": "query",
            "database": db_id,
            "query": {
                "source-table": table_id
            }
        },
        "display": "bar",  # Bar chart
        "visualization_settings": visualization_settings
    }
    logging.info(f"Payload for creating question '{query_config['name']}': {payload}")

    try:
        res = requests.post(f"{METABASE_URL}/api/card", json=payload, headers=headers)
        res.raise_for_status()
        card_id = res.json()['id']
        logging.info(f"Created question (card) with ID: {card_id}")
        return card_id
    except HTTPError as e:
        logging.error(f"Failed to create card: {e}, Response: {res.text}")
        raise

def create_dashboard_with_card(session, card_id, dashboard_name):
    headers = {"X-Metabase-Session": session}
    payload = {
        "name": dashboard_name,
        "dashcards": [
            {
                "card_id": card_id,
                "size_x": 6,
                "size_y": 4,
                "col": 0,
                "row": 0
            }
        ]
    }
    logging.info(f"Payload for creating dashboard '{dashboard_name}' with card: {payload}")
    try:
        res = requests.post(f"{METABASE_URL}/api/dashboard", json=payload, headers=headers)
        res.raise_for_status()
        dashboard = res.json()
        dashboard_id = dashboard['id']
        logging.info(f"Created dashboard '{dashboard_name}' with ID: {dashboard_id}, Response: {dashboard}")
        return dashboard_id
    except HTTPError as e:
        logging.error(f"Failed to create dashboard: {e}, Response: {res.text}")
        raise

def enable_auto_refresh(dashboard_id, session):
    headers = {"X-Metabase-Session": session}
    payload = {
        "auto_apply_filters": True,
        "refresh_interval": 60
    }
    try:
        res = requests.put(f"{METABASE_URL}/api/dashboard/{dashboard_id}", json=payload, headers=headers)
        res.raise_for_status()
        logging.info(f"Enabled auto-refresh on dashboard {dashboard_id}.")
    except HTTPError as e:
        logging.error(f"Failed to enable auto-refresh: {e}, Response: {res.text}")
        raise

dashboard_configs = [
    {
        "name": "Engagement Overview",
        "table": "ENGAGEMENT_OVERVIEW",
        "columns": ["PLATFORM", "ENGAGEMENT", "COMMUNITYENGAGEMENT", "LIVEENGAGEMENT"],
        "x_axis": "PLATFORM",
        "y_axis": "ENGAGEMENT"
    },
    {
        "name": "Community Trends",
        "table": "COMMUNITY_TRENDS",
        "columns": ["COMMUNITYNAME", "COMMUNITYENGAGEMENT", "TOTALTIMESPENT"],
        "x_axis": "COMMUNITYNAME",
        "y_axis": "COMMUNITYENGAGEMENT"
    },
    {
        "name": "Live Streaming",
        "table": "LIVE_STREAMING",
        "columns": ["DEVICETYPE", "LIVEENGAGEMENT", "STREAMDURATION", "VIEWERCOUNT"],
        "x_axis": "DEVICETYPE",
        "y_axis": "LIVEENGAGEMENT"
    },
    {
        "name": "Video Interactions",
        "table": "VIDEO_INTERACTIONS",
        "columns": ["WATCHREASON", "ENGAGEMENT", "PRODUCTIVITYLOSS", "SATISFACTION"],
        "x_axis": "WATCHREASON",
        "y_axis": "ENGAGEMENT"
    },
    {
        "name": "Time Trends",
        "table": "TIME_TRENDS",
        "columns": ["HOUR", "ENGAGEMENT", "TOTALTIMESPENT", "NUMBEROFSESSIONS"],
        "x_axis": "HOUR",
        "y_axis": "ENGAGEMENT"
    },
    {
        "name": "Demographics",
        "table": "DEMOGRAPHICS",
        "columns": ["AGEGROUP", "GENDER", "ENGAGEMENT", "PRODUCTIVITYLOSS"],
        "x_axis": "AGEGROUP",
        "y_axis": "ENGAGEMENT"
    }
]

try:
    session = login()

    for config in dashboard_configs:
        db_id, _ = get_database_and_table(session, config["table"])
        card_id = create_question(session, db_id, config["table"], config)
        dashboard_id = create_dashboard_with_card(session, card_id, config["name"])
        enable_auto_refresh(dashboard_id, session)
        print(f" Dashboard '{config['name']}' created with bar chart successfully.")

except Exception as e:
    logging.error(f"Error: {e}")
    print(f" Error: {e}")