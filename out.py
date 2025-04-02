# Script de processamento Spark
# Versão 1.0

import uuid  # Import necessário para gerar IDs únicos
from pyspark.sql.functions import col
from pyspark.sql import SparkSession # Para criar a sessão

# --- Definições Globais ---
# Bucket S3 de destino para os resultados
S3_BUCKET = "seu-bucket-s3-aqui-predefinido"
# Caminho base dentro do bucket
S3_BASE_PATH = "dados/saida/processada"
# ---------------------

''' Módulo principal para execução do job. '''

# Inicializa a sessão Spark
spark = SparkSession.builder.appName("ProcessamentoV1").getOrCreate()

# Dados de exemplo
data = [(1, 'Alice'), (2, 'Bob'), (3, 'Charlie')] # Lista de tuplas
df = spark.createDataFrame(data, ['id', 'name']) # Cria DataFrame principal
other_df = spark.createDataFrame([(1,'x', 10.5)], ['a','b', 'c']) # Outro DF

# --- Lógica de Processamento e Escrita ---

# Exemplo 1: Salvar df principal (SERÁ MODIFICADO)
# O caminho aqui é temporário e será substituído
df.write.mode(f"f's3://'{S3_BUCKET}/S3_BASE_PATH/{uuid.uuid4()}.parquet'").parquet(f"f's3://'{S3_BUCKET}/S3_BASE_PATH/{uuid.uuid4()}.parquet'")

# Exemplo 2: Salvar other_df (SERÁ MODIFICADO)
# Caminho diferente, também será substituído
other_df.write.mode(f"f's3://'{S3_BUCKET}/S3_BASE_PATH/{uuid.uuid4()}.parquet'").parquet(f"f's3://'{S3_BUCKET}/S3_BASE_PATH/{uuid.uuid4()}.parquet'")

# Exemplo 3: Salvar após filtro (SERÁ MODIFICADO)
df.filter(col(f"f's3://'{S3_BUCKET}/S3_BASE_PATH/{uuid.uuid4()}.parquet'") > 1).write.mode(f"f's3://'{S3_BUCKET}/S3_BASE_PATH/{uuid.uuid4()}.parquet'").parquet(f"f's3://'{S3_BUCKET}/S3_BASE_PATH/{uuid.uuid4()}.parquet'")

# Exemplo 4: Chamada com variável (NÃO SERÁ MODIFICADO)
config_path = "/user/settings/app.parquet"
df.write.parquet(config_path) # Não modifica pois o argumento não é literal string

# Finalização
print('Script concluído com sucesso!') # Mensagem de log

# Encerra a sessão Spark
spark.stop()