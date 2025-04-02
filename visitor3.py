import libcst as cst
import libcst.matchers as m
from typing import Optional

# --- Configuração (para construção da f-string) ---
TARGET_METHOD_NAME = 'parquet'
S3_BUCKET_VAR_NAME = "S3_BUCKET"
S3_BASE_PATH_VAR_NAME = "S3_BASE_PATH"

# --- Código Fonte Original (O mesmo com comentários) ---
original_code_with_deps_and_comments = """
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
df.write.mode('overwrite').parquet('/tmp/spark_output/result.parquet')

# Exemplo 2: Salvar other_df (SERÁ MODIFICADO)
# Caminho diferente, também será substituído
other_df.write.mode('append').parquet('/data/landing_zone/data_extra.parquet')

# Exemplo 3: Salvar após filtro (SERÁ MODIFICADO)
df.filter(col("id") > 1).write.mode('ignore').parquet("output_filtrado.parquet")

# Exemplo 4: Chamada com variável (NÃO SERÁ MODIFICADO)
config_path = "/user/settings/app.parquet"
df.write.parquet(config_path) # Não modifica pois o argumento não é literal string

# Finalização
print('Script concluído com sucesso!') # Mensagem de log

# Encerra a sessão Spark
spark.stop()
"""


# --- Transformador com libcst ---
class ParquetPathTransformer(m.MatcherDecoratableTransformer):

    # Usamos matchers para encontrar o padrão exato que queremos
    # Queremos: uma chamada onde a função é um atributo chamado 'parquet'
    #          e o primeiro argumento é uma string simples.
    @m.call_if_inside(m.Call(
        func=m.Attribute(attr=m.Name(TARGET_METHOD_NAME)),
        args=[m.Arg(value=m.SimpleString(m.MatchIfTrue(lambda v: True)))] # Qualquer SimpleString
    ))
    def leave_SimpleString(self, original_node: cst.SimpleString, updated_node: cst.SimpleString) -> cst.BaseExpression:
        # Este método é chamado QUANDO saímos de um SimpleString que
        # estava DENTRO da chamada .parquet() que definimos no matcher.

        original_path_value = original_node.evaluated_value # Pega o valor da string
        print(f"[*] LibCST: Modificando alvo: .{TARGET_METHOD_NAME}('{original_path_value}')")

        # Constrói a nova f-string usando nós do libcst
        new_fstring = cst.FormattedString(parts=[
            cst.FormattedStringText(value=f"s3://"), # f' prefixo + literal
            cst.FormattedStringExpression(expression=cst.Name(value=S3_BUCKET_VAR_NAME)),
            cst.FormattedStringText(value=f'/{S3_BASE_PATH_VAR_NAME}/'),
            cst.FormattedStringExpression(expression=cst.Call(
                 func=cst.Attribute(value=cst.Name("uuid"), attr=cst.Name("uuid4"))
            )),
            cst.FormattedStringText(value=f".parquet") # .parquet/ e fecha aspas
        ])

        print(f"[*] LibCST: Argumento substituído por f-string S3 dinâmica.")
        print(f"    (ATENÇÃO: Assume-se que 'uuid', '{S3_BUCKET_VAR_NAME}', e '{S3_BASE_PATH_VAR_NAME}' estão definidos)")

        # Retorna o novo nó f-string para substituir a string original
        return new_fstring

# --- Processo de Transformação com libcst ---
print("\n\n--- Usando 'libcst' para transformar (preserva comentários) ---")
try:
    # 1. Parse o código original (preserva tudo, incluindo comentários e espaços)
    source_tree = cst.parse_module(original_code_with_deps_and_comments)

    # 2. Instancia e aplica o transformador
    transformer = ParquetPathTransformer()
    modified_tree = source_tree.visit(transformer)

    # 3. Gera o código modificado
    print("\n--- Código Transformado (Comentários Preservados) ---")
    print(modified_tree.code)

except ImportError:
    print("\nErro: Biblioteca 'libcst' não encontrada.")
    print("Para usar esta opção (que preserva comentários), instale com: pip install libcst")
except Exception as e:
     print(f"\nErro durante a transformação com libcst: {e}")