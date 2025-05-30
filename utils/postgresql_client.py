import pandas as pd
import psycopg2
from sqlalchemy import create_engine

class PostgresSQLClient:
    def __init__(self, 
                 database, 
                 user, 
                 password, 
                 host = "127.0.0.1",
                 port = "5432"):
        self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.port = port
    
    def create_conn(self):
        conn = psycopg2.connect(
            database = self.database,
            user = self.user,
            password = self.password,
            host = self.host,
            port = self.port
        )
        # Creating a cursor object using the cursor() method
        return conn
    
    def get_sqlalchemy_engine(self):
        return create_engine(
            f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        )
    
    def execute_query(self, query):
        conn = self.create_conn()
        cursor = conn.cursor()
        cursor.execute(query)
        print(f"Query has been executed successfully!")
        conn.commit()
        # Closing the connection
        conn.close()

    def get_columns(self, table_name):
        try:
            engine = create_engine(
                f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
            )
            conn = engine.connect()
            df = pd.read_sql(f"SELECT * FROM {table_name}", conn)  # Use LIMIT 0 to avoid fetching data
            return df.columns.tolist()
        except Exception as e:
            print(f"Failed to get columns with error: {e}")
            return []