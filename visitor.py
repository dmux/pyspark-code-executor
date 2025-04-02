import ast
import sys

# --- Configuração da Transformação ---
# Não precisamos mais de um mapa de caminhos específicos.
# Vamos focar apenas no método .parquet() recebendo uma string literal.
TARGET_METHOD_NAME = 'parquet'
S3_BUCKET_NAME = "seu-bucket-s3-aqui" # <<< SUBSTITUA
S3_BASE_FOLDER = "data/parquet_output" # <<< Ajuste o caminho base conforme necessário

# --- Código Fonte Original (com diversos caminhos) ---
original_code = """
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
# import uuid # Será adicionado se necessário
# S3_BUCKET = "outro-bucket" # Pode já existir
# S3_BASE_PATH = "outra/base" # Pode já existir

''' Módulo para processar dados. '''

from __future__ import print_function # Exemplo de import futuro

spark = SparkSession.builder.appName("ExemploUniversal").getOrCreate()

data = [(1, 'Alice'), (2, 'Bob'), (3, 'Charlie')]
df = spark.createDataFrame(data, ['id', 'name'])
other_df = spark.createDataFrame([(1,'x')], ['a','b'])
config_df = spark.createDataFrame([("conf1", "/local/config")], ["key", "value"])

# Caminho específico - SERÁ MODIFICADO
df.write.mode('overwrite').parquet('/tmp/spark_output/result.parquet')

# Outro caminho específico - SERÁ MODIFICADO
other_df.write.mode('append').parquet('/data/landing_zone/file.parquet')

# Caminho genérico - SERÁ MODIFICADO
df.filter(col("id") > 1).write.mode('ignore').parquet("output.parquet")

# Chamada com variável - NÃO SERÁ MODIFICADO
config_path = "/user/settings/app.parquet"
df.write.parquet(config_path)

# Chamada com argumento nomeado (path=...) - NÃO SERÁ MODIFICADO pela lógica atual
# df.write.mode("overwrite").parquet(path="/another/output.parquet")

# Chamada de outro método com string - NÃO SERÁ MODIFICADO
df.write.format("delta").save('/tmp/delta_table')
config_df.write.csv("/path/to/config.csv")


print('Script concluído!')

spark.stop()
"""

# --- Classe Transformadora AST Universal ---
class S3UniversalParquetTransformer(ast.NodeTransformer):
    def __init__(self, target_method, s3_bucket, s3_base):
        self.target_method = target_method
        self.s3_bucket = s3_bucket
        self.s3_base = s3_base
        self._uuid_imported = False
        self._s3_bucket_defined = False
        self._s3_base_defined = False
        self._module_visited = False # Para evitar re-processar imports/vars

    def _create_s3_fstring_node(self):
        """Cria o nó AST para a f-string S3 dinâmica."""
        # f"s3a://{S3_BUCKET}/{S3_BASE_PATH}/{uuid.uuid4()}.parquet/"
        # Nota: Adicionamos .parquet antes da / final para clareza,
        # embora Spark crie diretórios. Ajuste se preferir sem a extensão aqui.
        return ast.JoinedStr(values=[
            ast.Constant(value=f"s3a://"),
            ast.FormattedValue(value=ast.Name(id='S3_BUCKET', ctx=ast.Load()), conversion=-1),
            ast.Constant(value=f"/{self.s3_base}/"),
            ast.FormattedValue(
                value=ast.Call(
                    func=ast.Attribute(value=ast.Name(id='uuid', ctx=ast.Load()), attr='uuid4', ctx=ast.Load()),
                    args=[], keywords=[]
                ),
                conversion=-1 # Sem !s, !r, !a
            ),
            # ast.Constant(value=".parquet/") # Opção 1: Nome do dir inclui .parquet
            ast.Constant(value="/")       # Opção 2: Nome do dir é só UUID
        ])

    def visit_Module(self, node):
        """Verifica e adiciona importações e definições de variáveis S3 ausentes."""
        if self._module_visited:
             return self.generic_visit(node)
        self._module_visited = True

        has_uuid_import = False
        has_s3_bucket_assign = False
        has_s3_base_assign = False

        # Verifica o corpo existente
        for item in node.body:
            # Checa por 'import uuid'
            if isinstance(item, ast.Import):
                if any(alias.name == 'uuid' for alias in item.names):
                    has_uuid_import = True
            # Checa por 'from uuid import ...'
            elif isinstance(item, ast.ImportFrom):
                 if item.module == 'uuid':
                      has_uuid_import = True
            # Checa por atribuições S3_BUCKET = ... e S3_BASE_PATH = ...
            elif isinstance(item, ast.Assign):
                for target in item.targets:
                    if isinstance(target, ast.Name):
                        if target.id == 'S3_BUCKET': has_s3_bucket_assign = True
                        elif target.id == 'S3_BASE_PATH': has_s3_base_assign = True

        insertions = []
        # Adiciona 'import uuid' se não encontrado
        if not has_uuid_import:
            insertions.append(ast.Import(names=[ast.alias(name='uuid')]))
            self._uuid_imported = True
            print("[*] Adicionando 'import uuid'")

        # Adiciona definições de variáveis S3 se não encontradas
        if not has_s3_bucket_assign:
            insertions.append(ast.Assign(
                targets=[ast.Name(id='S3_BUCKET', ctx=ast.Store())],
                value=ast.Constant(value=self.s3_bucket) # Python 3.8+
                # Para < 3.8 seria ast.Str(s=self.s3_bucket)
            ))
            self._s3_bucket_defined = True
            print(f"[*] Adicionando 'S3_BUCKET = \"{self.s3_bucket}\"'")
        if not has_s3_base_assign:
             insertions.append(ast.Assign(
                 targets=[ast.Name(id='S3_BASE_PATH', ctx=ast.Store())],
                 value=ast.Constant(value=self.s3_base) # Python 3.8+
                 # Para < 3.8 seria ast.Str(s=self.s3_base)
             ))
             self._s3_base_defined = True
             print(f"[*] Adicionando 'S3_BASE_PATH = \"{self.s3_base}\"'")

        # Encontra onde inserir (após docstring e __future__ imports)
        first_code_node_index = 0
        for i, item in enumerate(node.body):
            is_docstring = isinstance(item, ast.Expr) and isinstance(item.value, ast.Constant) and isinstance(item.value.value, str)
            is_future_import = isinstance(item, ast.ImportFrom) and item.module == '__future__'
            if not (is_docstring or is_future_import):
                first_code_node_index = i
                break
        # Insere as novas definições/imports
        node.body[first_code_node_index:first_code_node_index] = insertions

        # Continua visitando o resto da árvore
        return self.generic_visit(node)

    def visit_Call(self, node):
        """Visita nós de chamada e modifica chamadas a `.parquet('literal_string')`."""
        # Processa recursivamente primeiro
        node = self.generic_visit(node)

        # Verifica: é uma chamada de método? (ex: obj.method())
        if not isinstance(node.func, ast.Attribute):
            return node

        # Verifica: o nome do método é o nosso alvo? (ex: 'parquet')
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

        # Checagem compatível com Python >= 3.8 e < 3.8
        if isinstance(first_arg, ast.Constant) and isinstance(first_arg.value, str):
            is_string_literal = True
            original_path_value = first_arg.value
        elif isinstance(first_arg, ast.Str): # Para Python < 3.8
             is_string_literal = True
             original_path_value = first_arg.s # O atributo é 's' em ast.Str

        # Se não for uma string literal, não fazemos nada
        if not is_string_literal:
            return node

        # --- Condições Satisfeitas: Modificar o Argumento ---
        print(f"[*] Encontrado alvo: .{method_name}('{original_path_value}')")

        # Cria o nó da f-string S3 dinâmica
        s3_fstring_node = self._create_s3_fstring_node()

        # Substitui o nó do argumento original (ast.Constant ou ast.Str)
        # pelo nó da f-string (ast.JoinedStr)
        node.args[0] = s3_fstring_node
        print(f"[*] Argumento substituído por f-string S3 dinâmica.")

        # Opcional: Log sobre dependências adicionadas ou existentes
        dep_status = []
        if self._uuid_imported: dep_status.append("uuid import adicionado")
        if self._s3_bucket_defined: dep_status.append("S3_BUCKET definido")
        if self._s3_base_defined: dep_status.append("S3_BASE_PATH definido")
        if dep_status: print(f"    (Dependências: {'; '.join(dep_status)})")

        return node

# --- Processo de Transformação ---

# 1. Parse o código original
print("--- Analisando código original ---")
try:
    tree = ast.parse(original_code)
except SyntaxError as e:
    print(f"Erro de sintaxe no código original: {e}")
    exit(1)

# 2. Crie e aplique o transformador
print("\n--- Aplicando transformação ---")
transformer = S3UniversalParquetTransformer(
    target_method=TARGET_METHOD_NAME,
    s3_bucket=S3_BUCKET_NAME,
    s3_base=S3_BASE_FOLDER
)
new_tree = transformer.visit(tree)

# 3. Corrija informações de localização (essencial para unparse)
ast.fix_missing_locations(new_tree)

# 4. Gere o novo código fonte
print("\n--- Gerando código transformado ---")
new_code = ""
try:
    # Tenta usar ast.unparse (Python 3.9+)
    if sys.version_info >= (3, 9):
        new_code = ast.unparse(new_tree)
    else:
        # Fallback para astor (Python < 3.9)
        try:
            import astor
            new_code = astor.to_source(new_tree)
            print("[Info] Usando 'astor' para gerar código (Python < 3.9).")
        except ImportError:
            print("\nErro Crítico: Biblioteca 'astor' não encontrada.")
            print("Para Python < 3.9, por favor instale com: pip install astor")
            exit(1)

    print("\n--- Código Transformado ---")
    print(new_code)

except Exception as e:
    print(f"Erro durante a geração do código: {e}")
    print("\n--- AST Modificada (para depuração) ---")
    try:
        # Tenta exibir a AST para ajudar a depurar o erro
        print(ast.dump(new_tree, indent=2))
    except Exception as dump_e:
        print(f"Não foi possível exibir o dump da AST: {dump_e}")