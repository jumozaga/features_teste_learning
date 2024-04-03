import duckdb

# Conectar ao banco de dados DuckDB
conn = duckdb.connect('bancoemmemoria.db')

# Ler o arquivo CSV
df = conn.execute("SELECT * FROM read_csv('teste.csv')").fetchdf()

print(df)

# Fechar a conexão
conn.close()



cursor = duckdb.connect()
print(cursor.execute('SELECT 42').fetchall())