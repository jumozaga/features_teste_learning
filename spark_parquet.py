from pyspark.sql import SparkSession, Row

spark = SparkSession.builder.appName("demo-app").getOrCreate()


pessoas_df = spark.createDataFrame([
    Row(nome="Pedro", idade=15),
    Row(nome="João", idade=30),
    Row(nome="Maria", idade=19),
    Row(nome="Marcelo", idade=18),
    Row(nome="Alex", idade=38),
    Row(nome="Otavio", idade=44),
    Row(nome="Ricardo", idade=23),
    Row(nome="Camila", idade=12),
    Row(nome="Alice", idade=24),
    Row(nome="Marlei", idade=32),
    Row(nome="Marilene", idade=56),
    Row(nome="Judite", idade=60),
])

pessoas_df.write.parquet("pessoas2.parquet")

parquet_df = spark.read.parquet("pessoas2.parquet")

parquet_df.createOrReplaceTempView("pessoasView")


todas_pessoas = spark.sql("SELECT nome, idade FROM pessoasView")
todas_pessoas.show()

