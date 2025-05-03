import sqlite3
import pandas as pd

class SQLRepository:
    def __init__(self, connection):
        self.connection = connection
        
    def show_db_overview(self):
        cursor = self.connection.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables_indatabase = cursor.fetchall()
        print(tables_indatabase)
        
    def load_data(self, table_name, limit=None):
        if limit:
            sql = f"SELECT * FROM '{table_name}' LIMIT {limit}"
        else:
            sql = f"SELECT * FROM '{table_name}'"
        
        df = pd.read_sql(
            sql=sql,
            con=self.connection
            )
        
        return df
    
connection = sqlite3.connect(
    database="../etl/weather.db",
    check_same_thread=False
    )

repo = SQLRepository(connection)

repo.show_db_overview()

df = repo.load_data(table_name='weather_data', limit=None)
df
