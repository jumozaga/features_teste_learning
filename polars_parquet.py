import polars as pl
import time
import logging

# Configuração do logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Criação do DataFrame
pessoas_df = pl.DataFrame({
    "nome": ["Pedro", "João", "Maria", "Marcelo", "Alex", "Otavio", "Ricardo", "Camila", "Alice", "Marlei", "Marilene", "Judite"],
    "idade": [15, 30, 19, 18, 38, 44, 23, 12, 24, 32, 56, 60]
})

# Início do cronômetro
start_time_polars = time.time()

# Escrevendo o DataFrame para o arquivo Parquet
pessoas_df.write_parquet("pessoas_polars.parquet")

# Fim do cronômetro
end_time_polars = time.time()

# Calculando e registrando o tempo decorrido
time_taken_polars = end_time_polars - start_time_polars
logging.info(f"Tempo para criar o arquivo Parquet com Polars: {time_taken_polars} segundos")

# Lendo o arquivo Parquet
parquet_df_polars = pl.read_parquet("pessoas_polars.parquet")

# Exibindo o DataFrame lido
print(parquet_df_polars)
