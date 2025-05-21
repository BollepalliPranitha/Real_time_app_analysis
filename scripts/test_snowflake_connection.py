import configparser
import logging
from snowflake.connector import connect

# Configure logging
logging.basicConfig(
    filename='test_snowflake_connection.log',  # Save log in current directory
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

try:
    # Load configuration
    config = configparser.ConfigParser()
    config.read('snowflake_config.ini')  # Adjust path if config is elsewhere

    snowflake_user = config['snowflake']['user']
    snowflake_password = config['snowflake']['password']
    snowflake_account = config['snowflake']['account']
    snowflake_warehouse = config['snowflake']['warehouse']
    snowflake_database = config['snowflake']['database']
    snowflake_schema = config['snowflake']['schema']
    snowflake_role = config['snowflake']['role']

    logger.info(f"Connecting to Snowflake: account={snowflake_account}, user={snowflake_user}")

    # Connect to Snowflake
    conn = connect(
        user=snowflake_user,
        password=snowflake_password,
        account=snowflake_account,
        warehouse=snowflake_warehouse,
        database=snowflake_database,
        schema=snowflake_schema,
        role=snowflake_role
    )
    cursor = conn.cursor()
    cursor.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()")
    result = cursor.fetchone()
    logger.info(f"Connection successful: {result}")
    print(result)
    conn.close()
except Exception as e:
    logger.error(f"Connection failed: {str(e)}")
    raise