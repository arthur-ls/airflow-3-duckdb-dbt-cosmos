import duckdb

db_path = 'soccer_db'
conn = duckdb.connect(database=db_path)

conn.execute(f"ATTACH '{db_path}' AS db;").fetchall()
conn.execute(f"USE {db_path};").fetchall()

df = conn.execute(f"SELECT * FROM main_staging.stg_camp_brasileiro_cartoes;").fetch_df()

print(df)
print(df.columns)
print(df.dtypes)