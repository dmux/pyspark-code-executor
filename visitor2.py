import ast
import sys

# --- Configuração (apenas para a f-string, não para adicionar ao código) ---
TARGET_METHOD_NAME = 'parquet'
# Estas variáveis SÃO usadas para CONSTRUIR a f-string,
# mas NÃO serão adicionadas ao código pelo transformador.
# O código original DEVE defini-las ou elas devem vir do ambiente.
S3_BUCKET_VAR_NAME = "S3_BUCKET"      # Nome esperado da variável no código original/ambiente
S3_BASE_PATH_VAR_NAME = "S3_BASE_PATH"  # Nome esperado da variável no código original/ambiente

# --- Código Fonte Original (Exemplo - DEVE ter as dependências) ---
original_code_with_deps = """
import uuid  # <-- ESSENCIAL: Já presente ou garantido externamente
from pyspark.sql.functions import col
from pyspark.sql import SparkSession

# --- Definições S3 ---
#     ESSENCIAL: Devem existir no código original ou ser injetadas no ambiente
S3_BUCKET = "seu-bucket-s3-aqui-predefinido"
S3_BASE_PATH = "dados/saida/predefinida"
# ---------------------

''' Módulo para processar dados. '''

spark = SparkSession.builder.appName("ExemploMinimo").getOrCreate()

data = [(1, 'Alice'), (2, 'Bob'), (3, 'Charlie')]
df = spark.createDataFrame(data, ['id', 'name'])
other_df = spark.createDataFrame([(1,'x')], ['a','b'])

# Caminho específico - SERÁ MODIFICADO
df.write.mode('overwrite').parquet('/tmp/spark_output/result.parquet')

# Outro caminho específico - SERÁ MODIFICADO
other_df.write.mode('append').parquet('/data/landing_zone/file.parquet')

# Caminho genérico - SERÁ MODIFICADO
df.filter(col("id") > 1).write.mode('ignore').parquet("output.parquet")

# Chamada com variável - NÃO SERÁ MODIFICADO
config_path = "/user/settings/app.parquet"
df.write.parquet(config_path)

print('Script concluído!')

spark.stop()
"""

# --- Classe Transformadora AST Mínima ---
class MinimalS3PathTransformer(ast.NodeTransformer):
    def __init__(self, target_method, s3_bucket_var, s3_base_var):
        self.target_method = target_method
        self.s3_bucket_var = s3_bucket_var
        self.s3_base_var = s3_base_var

    def _create_s3_fstring_node(self):
        """Cria o nó AST para a f-string S3, usando nomes de variáveis existentes."""
        # f"s3a://{S3_BUCKET_VAR_NAME}/{S3_BASE_PATH_VAR_NAME}/{uuid.uuid4()}/"
        return ast.JoinedStr(values=[
            ast.Constant(value=f"s3a://"),
            ast.FormattedValue(value=ast.Name(id=self.s3_bucket_var, ctx=ast.Load()), conversion=-1),
            ast.Constant(value=f"/"),
            ast.FormattedValue(value=ast.Name(id=self.s3_base_var, ctx=ast.Load()), conversion=-1),
             ast.Constant(value=f"/"),
            ast.FormattedValue(
                value=ast.Call(
                    func=ast.Attribute(value=ast.Name(id='uuid', ctx=ast.Load()), attr='uuid4', ctx=ast.Load()),
                    args=[], keywords=[]
                ),
                conversion=-1
            ),
            ast.Constant(value="/")
        ])

    # Não implementamos visit_Module para adicionar imports/vars

    def visit_Call(self, node):
        """Visita chamadas e modifica apenas .parquet('string_literal')."""
        # Processa recursivamente primeiro
        node = self.generic_visit(node)

        # Verifica: é uma chamada de método?
        if not isinstance(node.func, ast.Attribute):
            return node

        # Verifica: o nome do método é o alvo?
        method_name = node.func.attr
        if method_name != self.target_method:
            return node

        # Verifica: há argumentos posicionais?
        if not node.args:
            return node

        # Verifica: o primeiro argumento é uma string literal?
        first_arg = node.args[0]
        is_string_literal = False
        original_path_value = None

        if isinstance(first_arg, ast.Constant) and isinstance(first_arg.value, str):
            is_string_literal = True
            original_path_value = first_arg.value
        elif isinstance(first_arg, ast.Str): # Python < 3.8
             is_string_literal = True
             original_path_value = first_arg.s

        # Se for uma string literal, modifica
        if is_string_literal:
            print(f"[*] Modificando alvo: .{method_name}('{original_path_value}')")
            # Cria o nó da f-string S3
            s3_fstring_node = self._create_s3_fstring_node()
            # Substitui o argumento original
            node.args[0] = s3_fstring_node
            print(f"[*] Argumento substituído por f-string S3 dinâmica.")
            print(f"    (ATENÇÃO: Assume-se que 'uuid', '{self.s3_bucket_var}', e '{self.s3_base_var}' estão definidos no escopo)")


        return node

# --- Processo de Transformação ---

# Use o código que JÁ TEM as dependências
code_to_transform = original_code_with_deps

# 1. Parse o código original
print("--- Analisando código original ---")
try:
    tree = ast.parse(code_to_transform)
except SyntaxError as e:
    print(f"Erro de sintaxe no código original: {e}")
    exit(1)

# 2. Crie e aplique o transformador MÍNIMO
print("\n--- Aplicando transformação mínima ---")
transformer = MinimalS3PathTransformer(
    target_method=TARGET_METHOD_NAME,
    s3_bucket_var=S3_BUCKET_VAR_NAME,
    s3_base_var=S3_BASE_PATH_VAR_NAME
)
new_tree = transformer.visit(tree)

# 3. Corrija informações de localização
ast.fix_missing_locations(new_tree)

# 4. Gere o novo código fonte
print("\n--- Gerando código transformado ---")
new_code = ""
try:
    if sys.version_info >= (3, 9):
        new_code = ast.unparse(new_tree)
    else:
        try:
            import astor
            new_code = astor.to_source(new_tree)
        except ImportError:
            print("\nErro: 'astor' não instalado. Por favor, instale com `pip install astor` para Python < 3.9")
            exit(1)

    print("\n--- Código Transformado (Somente argumento do Parquet modificado) ---")
    print(new_code)

except Exception as e:
    print(f"Erro durante a geração do código: {e}")