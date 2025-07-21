import pyodbc
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SQLServerConnection:

    def __init__(self, server, database, username, password, port):
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.port = port
        self.connection = None
        self.cursor = None

    def connect(self):
        try:
            connection_string = (
                f"DRIVER={{ODBC Driver 18 for SQL Server}};"
                f"SERVER={self.server},{self.port};"
                f"DATABASE={self.database};"
                f"UID={self.username};"
                f"PWD={self.password};"
                f"TrustServerCertificate=yes;"
                f"Encrypt=yes;"
            )

            logger.info(f"Attempting to connect to SQL Server at {self.server}...")

            self.connection = pyodbc.connect(connection_string, timeout=30)
            self.cursor = self.connection.cursor()
            logger.info("Successfully connected to SQL Server database!")
            return True
        
        except pyodbc.Error as e:
            logger.error(f"Database connection error: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error during connection: {e}")
            return False
        
    def test_connection(self):
        try:
            if not self.connection:
                logger.error("No active connection found")
                return False
            
            self.cursor.execute("SELECT 1 AS test")
            result = self.cursor.fetchone()
            logger.info("Connection test successful!")
            return True
        
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False
        
    def get_table_info(self, table_name):
        try:
            query = '''
            SELECT 
                COLUMN_NAME,
                DATA_TYPE,
                IS_NULLABLE,
                CHARACTER_MAXIMUM_LENGTH
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
            '''

            self.cursor.execute(query, table_name)
            columns = self.cursor.fetchall()

            table_info = []
            for col in columns:
                table_info.append({
                    'column_name': col[0],
                    'data_type': col[1],
                    'is_nullable': col[2],
                    'max_length': col[3]
                })

            return table_info
        
        except Exception as e:
            logger.error(f"Error getting table info: {e}")
            return None
        
    def fetch_employees_data(self, limit=5):
        try:
            table_check_query = '''
            SELECT COUNT(*)
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_NAME = 'Employees'
            '''

            self.cursor.execute(table_check_query)
            table_exists = self.cursor.fetchone()[0]

            if table_exists == 0:
                logger.error("Table 'Employees' does not exist in the database")
                return None
            
            logger.info("Getting table structure...")
            table_info = self.get_table_info('Employees')
            if table_info:
                logger.info("Employees table structure:")
                for col in table_info:
                    logger.info(f"  - {col['column_name']}: {col['data_type']}")
                
            query = f"SELECT TOP {limit} * FROM Employees"
            df = pd.read_sql(query, self.connection)
            logger.info(f"Successfully retrieved {limit} rows from 'Employees' table")

            return df
        
        except Exception as e:
            logger.error(f"Error fetching employees data: {e}")
            return None
    
    def execute_custom_query(self, query):
        try:
            logger.info(f"Executing custom query: {query}")
            df = pd.read_sql(query, self.connection)
            logger.info(f"Query executed successfully, returned {len(df)} rows")
            return df
        
        except Exception as e:
            logger.error(f"Error executing custom query: {e}")
            return None
        
    def close_connection(self):
        try:
            if self.cursor:
                self.cursor.close()
            if self.connection:
                self.connection.close()
            logger.info("Database connection closed successfully")

        except Exception as e:
            logger.error(f"Error closing connection: {e}")

def main():
    SERVER = "tcp:nischal-server.database.windows.net"
    DATABASE = "test_db"
    USERNAME = "nischal"
    PASSWORD = "Paliwal@2004"
    PORT = 1433

    db = SQLServerConnection(SERVER, DATABASE, USERNAME, PASSWORD, PORT)

    try:
        if not db.connect():
            print("Failed to connect to the database. Please check your credentials and network connectivity.")
            return
        if not db.test_connection():
            print("Connection test failed.")
            return
        
        employees_df = db.fetch_employees_data(limit=5)

        if employees_df is not None:
            print(employees_df.to_string(index=False))
        else:
            print("No data received or table is empty.")
        
        count_query = "SELECT COUNT(*) as TotalEmployees FROM Employees"
        count_df = db.execute_custom_query(count_query)

        if count_df is not None:
            print(count_df.to_string(index=False))
        
    except KeyboardInterrupt:
        print("\nOperation cancelled by user.")
    except Exception as e:
        logger.error(f"Unexpected error in the main function: {e}")
    finally:
        db.close_connection()

if __name__ == "__main__":
    main()