import duckdb
import time
import logging

# Configuração do logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Criação do DataFrame
pessoas_df = [
    {"nome": "Pedro", "idade": 15},
    {"nome": "João", "idade": 30},
    {"nome": "Maria", "idade": 19},
    {"nome": "Marcelo", "idade": 18},
    {"nome": "Alex", "idade": 38},
    {"nome": "Otavio", "idade": 44},
    {"nome": "Ricardo", "idade": 23},
    {"nome": "Camila", "idade": 12},
    {"nome": "Alice", "idade": 24},
    {"nome": "Marlei", "idade": 32},
    {"nome": "Marilene", "idade": 56},
    {"nome": "Judite", "idade": 60},
]

# Conexão com o DuckDB
conn = duckdb.connect()

# Criação da tabela no DuckDB
conn.execute("CREATE TABLE pessoas (nome VARCHAR, idade INTEGER)")

# Inserção dos dados na tabela
for pessoa in pessoas_df:
    conn.execute("INSERT INTO pessoas VALUES (?, ?)", (pessoa['nome'], pessoa['idade']))

# Início do cronômetro
start_time_duckdb = time.time()

# Escrevendo a tabela para o arquivo Parquet
conn.execute("COPY pessoas TO 'pessoas_duckdb.parquet' (FORMAT 'parquet')")

# Fim do cronômetro
end_time_duckdb = time.time()

# Calculando e registrando o tempo decorrido
time_taken_duckdb = end_time_duckdb - start_time_duckdb
logging.info(f"Tempo para criar o arquivo Parquet com DuckDB: {time_taken_duckdb} segundos")

# Lendo o arquivo Parquet
parquet_df_duckdb = conn.execute("SELECT * FROM read_parquet('pessoas_duckdb.parquet')").fetchall()

# Exibindo os dados lidos
print(parquet_df_duckdb)
