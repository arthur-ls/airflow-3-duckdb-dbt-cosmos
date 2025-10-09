import duckdb
import data.extract.get_data as get_data

class DuckDBSaveDatabase:
    def __init__(self, db_path: str = ":memory:"):
        self.conn = duckdb.connect(database=db_path)

    def execute_query(self, query: str):
        result = self.conn.execute(query).fetchall()
        return result

    def close_connection(self):
        self.conn.close()

def main():
    db_test = DuckDBSaveDatabase(db_path="soccer_db.duckdb")
    query = "SELECT 42 AS answer"
    result = db_test.execute_query(query)
    print(f"Query Result: {result}")
    db_test.close_connection()

if __name__ == "__main__":
    main()