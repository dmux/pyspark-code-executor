import re

class PySparkPathTransformer:
    def __init__(self, bucket_name, transpilation_id):
        """
        Inicializa o transformador com informações do bucket e ID.

        Args:
            bucket_name (str): O nome do bucket S3 a ser usado no caminho.
            transpilation_id (str): O ID de transpilação a ser usado no caminho.
        """
        if not bucket_name or not transpilation_id:
            raise ValueError("Bucket name e Transpilation ID não podem ser vazios.")
        self._bucket_name = bucket_name
        self._transpilation_id = transpilation_id

    def _extrair_nome_base_limpo(self, caminho_arquivo):
        """
        Extrai o nome base limpo de um caminho de arquivo (método auxiliar).
        """
        if not caminho_arquivo:
            return None
        # Regex para extrair o nome base (sem a extensão e o caminho)
        padrao = r"(?:.*/)?([^.]+)\.[^/]*$"
        match = re.search(padrao, caminho_arquivo)
        if match:
            # Retorna o conteúdo do primeiro grupo de captura (o nome base)
            return match.group(1)
        else:
            # Se não houver correspondência (ex: string sem ponto/extensão), retorna None
            return None

    def substituir_caminhos_para_s3_tmp(self, codigo_pyspark):
        """
        Substitui strings literais de caminho de arquivo em um código PySpark
        pelo novo caminho S3 formatado: s3://{bucket}/{transp_id}/tmp/{nome_base}.

        Args:
            codigo_pyspark (str): A string contendo o código PySpark.

        Returns:
            str: O código PySpark com as substituições realizadas.
        """
        if not codigo_pyspark:
            return codigo_pyspark

        # Regex para encontrar strings literais (aspas simples ou duplas)
        padrao_string_literal = r'(["\'])(.+?)\1'

        def _replacer(match):
            """
            Função interna chamada por re.sub para cada string literal encontrada.
            """
            aspas = match.group(1)          # A aspa usada (' ou ")
            conteudo_original = match.group(2) # O texto dentro das aspas

            # Tenta extrair o nome base do conteúdo da string
            nome_base = self._extrair_nome_base_limpo(conteudo_original)

            if nome_base:
                # Se a extração foi bem-sucedida, monta a nova string S3
                novo_caminho_s3 = f"s3://{self._bucket_name}/{self._transpilation_id}/tmp/{nome_base}.parquet"
                # Retorna a nova string S3 dentro das mesmas aspas originais
                return f"{aspas}{novo_caminho_s3}{aspas}"
            else:
                # Se não foi possível extrair (não era um caminho no formato esperado)
                # ou se a extração retornou None, retorna a string literal original sem modificação.
                return match.group(0) # Retorna a correspondência inteira original

        # Aplica a substituição usando re.sub e a função _replacer
        codigo_modificado = re.sub(padrao_string_literal, _replacer, codigo_pyspark)

        return codigo_modificado

# --- Exemplo de Uso ---

# 1. Crie uma instância da classe com seu bucket e ID
meu_bucket = "meu-bucket-processamento"
meu_id_transpilacao = "execucao-12345"
transformer = PySparkPathTransformer(bucket_name=meu_bucket, transpilation_id=meu_id_transpilacao)


# 2. Use o método na sua string de código PySpark
codigo_exemplo_pyspark = """
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Processamento").getOrCreate()

# Leitura de dados
df_vendas = spark.read.parquet("s3://bucket-origem/raw/vendas_dia.parquet")
df_clientes = spark.read.csv('/dados/entrada/clientes.csv')
df_config = spark.read.json('{workspace}/configs/parametros.json')

# Processamento
df_processado = df_vendas.join(df_clientes, "id_cliente")

# Definição de caminhos de saída (serão substituídos)
caminho_saida_parquet = "s3://qualquer-coisa/output/resultado_final.parquet"
caminho_temporario_csv = "/tmp/staging/dados_temp.csv"
outro_arquivo = "local_file.txt" # Será substituído também

# Escrita (os caminhos aqui dentro serão alterados)
df_processado.write.mode("overwrite").parquet(caminho_saida_parquet)
df_processado.select("nome", "valor").write.csv(caminho_temporario_csv)
df_config.write.json(outro_arquivo)

print(f"Dados de config lidos de: '{workspace}/configs/parametros.json'") # Exemplo de f-string, não será alterado
print("Caminho de log: /var/log/processo.log") # Não será alterado
print('Usando arquivo: ' + outro_arquivo) # O valor da variável 'outro_arquivo' será substituído aqui

spark.stop()
"""

print("--- Código Original ---")
print(codigo_exemplo_pyspark)

codigo_transformado = transformer.substituir_caminhos_para_s3_tmp(codigo_exemplo_pyspark)

print(f"\n--- Código Transformado (Bucket: {meu_bucket}, ID: {meu_id_transpilacao}) ---")
print(codigo_transformado)

# Exemplo com código mais simples
codigo_simples = 'df = spark.read.load("s3://antigo/path/tabela_abc.parquet")'
codigo_simples_transformado = transformer.substituir_caminhos_para_s3_tmp(codigo_simples)
print("\n--- Código Simples Original ---")
print(codigo_simples)
print("\n--- Código Simples Transformado ---")
print(codigo_simples_transformado)
# Deve imprimir: df = spark.read.load("s3://meu-bucket-processamento/execucao-12345/tmp/tabela_abc")