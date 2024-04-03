import pandas as pd
import time
import logging

# Configuração do logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

pessoas_df = pd.DataFrame([
    {"nome":"Pedro", "idade": 15},
    {"nome":"João", "idade":30},
    {"nome":"Maria", "idade":19},
    {"nome":"Marcelo", "idade":18},
    {"nome":"Alex", "idade":38},
    {"nome":"Otavio", "idade":44},
    {"nome":"Ricardo", "idade":23},
    {"nome":"Camila", "idade":12},
    {"nome":"Alice", "idade":24},
    {"nome":"Marlei", "idade":32},
    {"nome":"Marilene", "idade":56},
    {"nome":"Judite", "idade":60},
])

# Início do cronômetro
start_time = time.time()

# Escrevendo o DataFrame para o arquivo Parquet
pessoas_df.to_parquet("pessoas_pandas.pq")

# Fim do cronômetro
end_time = time.time()

# Calculando e registrando o tempo decorrido
time_taken = end_time - start_time
logging.info(f"Tempo para criar o arquivo Parquet: {time_taken} segundos")

# Lendo o arquivo Parquet
parquet_df = pd.read_parquet("pessoas_pandas.pq")

# Exibindo o DataFrame lido
print(parquet_df)
