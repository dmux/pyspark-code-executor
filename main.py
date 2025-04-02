import os
import sys
import io
import re
import json
import shutil
import signal # Para timeouts (alternativa)
import threading # Para timeouts (alternativa)
import builtins
import importlib
from contextlib import contextmanager
from pathlib import Path
from typing import Optional, Any, List, Dict, Tuple, Set

from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends # Depends adicionado
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel, Field
import pandas as pd
import asyncio # Para timeouts com run_in_threadpool

# --- Imports e Inicialização do Spark (sem mudanças) ---
try:
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
    import pyspark.sql.functions as F # Importar funções comuns para disponibilizar
    from pyspark.errors import PySparkException

    print("Inicializando SparkSession global...")
    spark_instance = SparkSession.builder \
        .appName("FastAPI PySpark Executor") \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.legacy.createHiveTableByDefault", "false") \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .getOrCreate()
    print("SparkSession inicializada.")
    SC_instance = spark_instance.sparkContext
    print(f"Spark UI disponível em: {SC_instance.uiWebUrl}")

except ImportError:
    print("ERRO: PySpark não parece estar instalado ou configurável.")
    spark_instance = None
    PySparkException = Exception
    SparkDataFrame = None
    F = None # Definir como None se PySpark não estiver disponível
except Exception as e:
    print(f"ERRO ao inicializar SparkSession: {e}")
    spark_instance = None
    PySparkException = Exception
    SparkDataFrame = None
    F = None # Definir como None se PySpark não estiver disponível

# --- Constantes ---
DEFAULT_PARQUET_OUTPUT_DIR = Path("/tmp/spark_output")
DEFAULT_PARQUET_OUTPUT_PATH = DEFAULT_PARQUET_OUTPUT_DIR / "result.parquet"
# --- Constantes de Segurança ---
EXECUTION_TIMEOUT_SECONDS = 60  # Limite de tempo para a execução do código
MAX_PANDAS_ROWS = 10000        # Limite de linhas para .toPandas()
ALLOWED_MODULES: Set[str] = {
    'math', 'datetime', 'json', 're', 'collections', 'functools',
    'pyspark', # Permite submodulos como pyspark.sql.types etc
    'pyspark.sql.functions', # Permitir explicitamente F
    'pyspark.sql.types',
    # Adicione outros módulos que você considera seguros e necessários
}
DISALLOWED_BUILTINS: Set[str] = {
    'open', 'eval', 'exec', 'input', 'compile', '__import__', # Bloquear __import__ diretamente
    'globals', 'locals', 'vars', # Prevenir introspecção perigosa
    'exit', 'quit', # Prevenir saída abrupta
    # 'breakpoint', # Disponível no Python 3.7+
    # Funções que interagem com o sistema
    'print', # Substituiremos por uma versão segura se necessário, mas capturar stdout é melhor
}

# --- Modelos Pydantic (sem mudanças) ---
class PySparkCodeRequest(BaseModel):
    code: str = Field(..., description="O código PySpark a ser executado.")

class ExecutionResponse(BaseModel):
    status: str = Field(description="Status da execução ('success', 'error', 'timeout').")
    stdout: Optional[str] = Field(None, description="Saída padrão capturada da execução.")
    stderr: Optional[str] = Field(None, description="Saída de erro capturada da execução.")
    error_message: Optional[str] = Field(None, description="Mensagem de erro específica se a execução falhou.")
    result_source: Optional[str] = Field(None, description="Origem do resultado ('parquet', 'view', 'direct_dataframe', 'none', 'read_error', 'conversion_limit', 'json_serialization_error', 'timeout', 'security_error').")
    result_data_json: Optional[List[Dict[str, Any]]] = Field(None, description="Dados do resultado (DataFrame limitado convertido para JSON), como uma lista de dicionários.")
    detected_parquet_path: Optional[str] = Field(None, description="Caminho do arquivo Parquet detectado (se aplicável).")
    detected_view_name: Optional[str] = Field(None, description="Nome da View temporária detectada (se aplicável).")
    created_variables: Optional[Dict[str, str]] = Field(None, description="Nomes e representação de novas variáveis criadas pelo código (para debug).")


# --- Funções Auxiliares (cleanup_output, safe_repr - sem mudanças) ---
def cleanup_output():
    if DEFAULT_PARQUET_OUTPUT_DIR.exists():
        try:
            shutil.rmtree(DEFAULT_PARQUET_OUTPUT_DIR)
            print(f"Diretório de saída anterior limpo: {DEFAULT_PARQUET_OUTPUT_DIR}")
        except Exception as e:
            print(f"Aviso: Falha ao limpar diretório de saída anterior: {e}")
    DEFAULT_PARQUET_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def safe_repr(obj, length=100):
    try:
        r = repr(obj)
        if len(r) > length:
            return r[:length] + "..."
        return r
    except Exception:
        return f"<repr error for type {type(obj).__name__}>"

# --- Sandboxing Utilities ---

def create_safe_builtins() -> Dict[str, Any]:
    """Cria um dicionário de builtins seguros."""
    safe_builtins_dict = {
        name: obj for name, obj in builtins.__dict__.items()
        if name not in DISALLOWED_BUILTINS
    }
    # Adicionar um import seguro
    safe_builtins_dict['__import__'] = safe_importer
    # Adicionar 'print' de volta se não estiver na lista de proibidos,
    # ou potencialmente uma versão customizada que loga em vez de imprimir direto.
    # Por enquanto, capturaremos stdout/stderr, então remover 'print' não é estritamente
    # necessário, mas bloqueia a *função* print se alguém tentar usá-la maliciosamente.
    # Se precisar de print, remova-o de DISALLOWED_BUILTINS.
    # safe_builtins_dict['print'] = builtins.print # Exemplo se quiser permitir print
    return safe_builtins_dict

def safe_importer(name, globals=None, locals=None, fromlist=(), level=0):
    """
    Substituto para __import__ que permite apenas módulos em ALLOWED_MODULES.
    Permite import relativo e submodulos de módulos permitidos (e.g., pyspark.sql).
    """
    # Verifica o módulo base (e.g., 'os.path' -> 'os')
    base_module_name = name.split('.', 1)[0]

    # Permite imports relativos (level > 0) dentro do contexto do código executado
    # Isso é complexo de tornar seguro, então vamos simplificar por enquanto.
    # Uma abordagem mais segura poderia ser bloquear imports relativos (level != 0)
    # ou inspecionar o pacote de onde está sendo importado.
    # Por simplicidade e segurança inicial, vamos focar nos imports absolutos (level=0).
    if level != 0:
         raise ImportError(f"Imports relativos (level={level}) não são permitidos no ambiente seguro.")

    # Verifica se o módulo base está na lista de permissões
    if base_module_name not in ALLOWED_MODULES:
        raise ImportError(f"Import do módulo '{base_module_name}' (e seus submódulos) não é permitido.")

    # Se o módulo base é permitido, usa o importador padrão para carregar
    # Isso ainda carrega o código do módulo, então ele deve ser confiável.
    try:
        # Usar importlib para mais controle se necessário, mas __import__ padrão
        # respeitará o sys.path etc.
        module = importlib.__import__(name, globals, locals, fromlist, level)
        return module
    except ImportError as e:
        raise ImportError(f"Erro ao importar módulo permitido '{name}': {e}") from e
    except Exception as e:
        # Captura outros erros potenciais durante o import
         raise ImportError(f"Exceção inesperada ao importar '{name}': {e}") from e

# Dicionário global para builtins seguros (criado uma vez)
SAFE_BUILTINS = create_safe_builtins()
ALLOWED_GLOBALS = {
    'spark': spark_instance, # Passa a SESSÃO SPARK
    'F': F,                  # Passa as funções do PySpark (se importado com sucesso)
    'pd': pd,                # Permite usar Pandas para criar DFs Spark, etc. (opcional)
    # Adicione outras variáveis/funções globais seguras que o código possa precisar
}
# Remover None de ALLOWED_GLOBALS se algo falhou na inicialização
ALLOWED_GLOBALS = {k: v for k, v in ALLOWED_GLOBALS.items() if v is not None}
ALLOWED_GLOBALS['__builtins__'] = SAFE_BUILTINS # Crucial: injeta os builtins seguros


# --- Lógica de Execução MODIFICADA com Sandboxing ---
def execute_pyspark_code_sync(
    code_string: str, spark_session: SparkSession
) -> Tuple[str, str, Optional[Exception], Dict[str, Any], Optional[str], Optional[str], Optional[pd.DataFrame], str]:
    """
    Executa código PySpark SÍNCRONAMENTE em um ambiente mais restrito,
    captura saídas, detecta resultados, e TENTA converter o resultado
    (se for DataFrame) para Pandas, aplicando limites.

    Retorna: (
        stdout,
        stderr,
        execution_exception, # Erro durante o exec() ou pós-execução
        new_vars_dict,       # Dicionário com as variáveis criadas pelo exec
        detected_parquet,    # Path do parquet detectado
        detected_view,       # Nome da view detectada
        result_pandas_df,    # DataFrame Pandas resultante (ou None)
        result_source        # String indicando a origem/status
    )
    """
    if not spark_session:
        return "", "SparkSession não está disponível.", RuntimeError("SparkSession not initialized"), {}, None, None, None, "error"

    print("--- Código PySpark a ser executado (em ambiente restrito) ---")
    print(code_string)
    print("----------------------------------------------------------")
    cleanup_output()

    # Captura de stdout/stderr (como antes)
    old_stdout = sys.stdout
    old_stderr = sys.stderr
    redirected_output = io.StringIO()
    redirected_error = io.StringIO()
    sys.stdout = redirected_output
    sys.stderr = redirected_error

    # --- Preparação do Ambiente de Execução Restrito ---
    # Usamos uma cópia dos ALLOWED_GLOBALS para cada execução
    # para que as variáveis criadas pelo código não poluam o escopo global permitido.
    exec_globals = dict(ALLOWED_GLOBALS) # Usa os globals seguros definidos anteriormente
    exec_locals = {} # Locais começam vazios, serão populados pelo exec

    initial_keys = set(exec_locals.keys()) # Nenhum inicialmente
    initial_views = set(t.name for t in spark_session.catalog.listTables() if t.isTemporary)

    execution_exception: Optional[Exception] = None
    new_vars_dict: Dict[str, Any] = {}
    detected_parquet_path_str: Optional[str] = None
    detected_view_name: Optional[str] = None
    result_pandas_df: Optional[pd.DataFrame] = None
    result_source: str = "none"

    try:
        # --- Execução com Restrições ---
        # Nota: `exec` ainda pode ser perigoso. As restrições de builtins e imports ajudam,
        # mas código malicioso pode tentar explorar outras vulnerabilidades (e.g., na JVM via Spark).
        # O timeout é gerenciado na chamada da API (via asyncio.wait_for).
        exec(code_string, exec_globals, exec_locals) # Executa com globals e locals restritos

    except ImportError as e:
        print(f"\nErro de Import Proibido ou Inválido: {e}", file=sys.stderr)
        execution_exception = e
        result_source = "security_error" # Erro específico de segurança
    except PySparkException as e:
        print(f"\nErro PySpark durante a execução: {e}", file=sys.stderr)
        execution_exception = e
    except MemoryError as e:
         print(f"\nErro de Memória durante a execução (possível DoS): {e}", file=sys.stderr)
         execution_exception = e
         result_source = "memory_error" # Erro específico
    except Exception as e:
        # Captura outros erros (SyntaxError, NameError, TypeError, etc.)
        print(f"\nErro Python durante a execução: {e}", file=sys.stderr)
        execution_exception = e
    finally:
        # Restaura stdout/stderr independentemente do resultado
        sys.stdout = old_stdout
        sys.stderr = old_stderr
        stdout_output = redirected_output.getvalue()
        stderr_output = redirected_error.getvalue()

        # Captura Novas Variáveis (agora do `exec_locals`)
        final_keys = set(exec_locals.keys())
        new_keys = final_keys - initial_keys
        if new_keys:
            print(f"Variáveis criadas pela execução: {', '.join(new_keys)}")
            # Filtra para não incluir '__builtins__' se ele foi modificado localmente
            new_vars_dict = {key: exec_locals[key] for key in new_keys if key != '__builtins__'}

    # --- Tentativa de Leitura e Conversão PÓS-EXECUÇÃO (se não houve erro no exec) ---
    if execution_exception is None:
        spark_df_to_convert: Optional[SparkDataFrame] = None
        read_or_convert_error: Optional[Exception] = None

        try:
            # 1. Detecção de Parquet (sem mudanças na lógica de detecção)
            if DEFAULT_PARQUET_OUTPUT_PATH.exists() or any(DEFAULT_PARQUET_OUTPUT_DIR.iterdir()):
                 # ... (lógica de detecção de parquet como antes) ...
                 potential_path = next(DEFAULT_PARQUET_OUTPUT_DIR.iterdir(), None)
                 if DEFAULT_PARQUET_OUTPUT_PATH.exists():
                     detected_parquet_path_str = str(DEFAULT_PARQUET_OUTPUT_PATH.resolve())
                 elif potential_path:
                     # IMPORTANTE: Validar se o path está DENTRO de DEFAULT_PARQUET_OUTPUT_DIR
                     # para evitar leitura de arquivos arbitrários se o código malicioso
                     # conseguiu manipular o nome do diretório de alguma forma.
                     resolved_path = potential_path.resolve()
                     if resolved_path.is_relative_to(DEFAULT_PARQUET_OUTPUT_DIR.resolve()):
                        detected_parquet_path_str = str(resolved_path)
                     else:
                        print(f"AVISO DE SEGURANÇA: Path detectado fora do diretório permitido: {resolved_path}", file=sys.stderr)
                        stderr_output += f"\nAVISO DE SEGURANÇA: Tentativa de ler parquet fora do diretório permitido: {resolved_path}"
                        detected_parquet_path_str = None # Ignora

                 if detected_parquet_path_str:
                    print(f"Detectado Parquet em: {detected_parquet_path_str}. Tentando ler...")
                    spark_df_to_convert = spark_session.read.parquet(detected_parquet_path_str)
                    result_source = "parquet"

            # 2. Detecção de View (sem mudanças na lógica de detecção)
            if spark_df_to_convert is None:
                final_views = set(t.name for t in spark_session.catalog.listTables() if t.isTemporary)
                new_views = final_views - initial_views
                if len(new_views) == 1:
                    detected_view_name = new_views.pop()
                    print(f"Detectada nova View: {detected_view_name}. Tentando ler...")
                    spark_df_to_convert = spark_session.table(detected_view_name)
                    result_source = "view"
                elif len(new_views) > 1:
                    print(f"Aviso: Múltiplas Views criadas ({', '.join(new_views)}).")

            # 3. Detecção de DataFrame Direto (sem mudanças na lógica de detecção)
            if spark_df_to_convert is None and new_vars_dict:
                 print("Verificando variáveis criadas por um DataFrame Spark...")
                 # Tenta encontrar a última variável que seja um Spark DF
                 last_df_var = None
                 for var_name, var_value in reversed(new_vars_dict.items()):
                     if SparkDataFrame and isinstance(var_value, SparkDataFrame):
                         last_df_var = var_name
                         break

                 if last_df_var:
                     print(f"Encontrado Spark DataFrame na variável: '{last_df_var}'")
                     spark_df_to_convert = new_vars_dict[last_df_var]
                     result_source = "direct_dataframe"
                 else:
                      print("Nenhuma variável nova era um Spark DataFrame.")


            # 4. Conversão para Pandas com Limite (se um DF Spark foi encontrado)
            if spark_df_to_convert is not None:
                print(f"Convertendo DataFrame Spark (origem: {result_source}) para Pandas com limite de {MAX_PANDAS_ROWS} linhas...")
                try:
                    # Aplica o limite ANTES de toPandas()
                    # .limit() é uma transformação, então é relativamente barata.
                    # .count() pode ser caro, então limitar primeiro é melhor.
                    limited_spark_df = spark_df_to_convert.limit(MAX_PANDAS_ROWS + 1) # +1 para detectar se excedeu

                    # ------ AVISO DE MEMÓRIA: .toPandas() ------
                    result_pandas_df = limited_spark_df.toPandas()
                    # ------------------------------------------

                    if len(result_pandas_df) > MAX_PANDAS_ROWS:
                        print(f"AVISO: Resultado excedeu o limite de {MAX_PANDAS_ROWS} linhas. Truncando.")
                        result_pandas_df = result_pandas_df.head(MAX_PANDAS_ROWS)
                        # Atualiza source para indicar que foi limitado
                        result_source = result_source + "_limited" if result_source != "none" else "direct_dataframe_limited"
                        stderr_output += f"\nAviso: Resultado convertido para Pandas foi limitado a {MAX_PANDAS_ROWS} linhas."

                    print(f"Conversão para Pandas concluída. Shape resultante: {result_pandas_df.shape}")

                except MemoryError as me:
                     print(f"\nErro de Memória ao converter para Pandas (mesmo com limite?): {me}", file=sys.stderr)
                     read_or_convert_error = me
                     result_source = "memory_error_pandas" # Erro específico
                     stderr_output += f"\n--- Erro Pós-Execução (Conversão Pandas) ---\n{me}"
                     result_pandas_df = None # Garante que não há resultado parcial

            elif result_source == "none":
                 print("Nenhum resultado (Parquet, View, DataFrame direto) detectado para conversão.")

        except PySparkException as e:
            print(f"\nErro PySpark ao LER/CONVERTER resultado: {e}", file=sys.stderr)
            stderr_output += f"\n--- Erro Pós-Execução (Leitura/Conversão) ---\n{e}"
            read_or_convert_error = e
            result_source = "read_error"
        except Exception as e:
            print(f"\nErro Python ao LER/CONVERTER resultado: {e}", file=sys.stderr)
            stderr_output += f"\n--- Erro Pós-Execução (Leitura/Conversão) ---\n{e}"
            read_or_convert_error = e
            result_source = "read_error"

        # Se houve erro na leitura/conversão, anexa ao erro da execução (se houver)
        # ou o torna o erro principal se a execução foi OK.
        if read_or_convert_error and not execution_exception:
             execution_exception = read_or_convert_error

    print("--- Fim da Execução Síncrona ---")

    # Se a fonte ainda for 'none' e não houve exceção, significa execução ok sem resultado detectável
    if execution_exception is None and result_source == "none":
        print("Execução concluída com sucesso, mas nenhum resultado detectado.")


    return (
        stdout_output,
        stderr_output,
        execution_exception,
        new_vars_dict,
        detected_parquet_path_str,
        detected_view_name,
        result_pandas_df, # O objeto Pandas DataFrame (ou None)
        result_source
    )


# --- Aplicação FastAPI ---
app = FastAPI(
    title="PySpark Code Executor API (Sandboxed)",
    description="Executa código PySpark em um ambiente *parcialmente* restrito. **AVISO:** Embora medidas de segurança tenham sido implementadas (restrição de builtins/imports, timeouts, limites de resultado), executar código arbitrário ainda apresenta riscos inerentes. Use com extrema cautela e apenas com código de fontes confiáveis ou após análise rigorosa.",
    version="0.2.0", # Versão incrementada
)

# --- Dependência para obter SparkSession ---
# Isso garante que verificamos se o Spark está disponível em cada request
# e simplifica a passagem para o endpoint.
async def get_spark_session():
    if spark_instance is None:
        raise HTTPException(status_code=503, detail="SparkSession não está disponível.")
    return spark_instance

# --- Eventos startup/shutdown (sem mudanças) ---
@app.on_event("startup")
async def startup_event():
    if spark_instance is None:
        print("AVISO: SparkSession não foi inicializada na inicialização.")
    DEFAULT_PARQUET_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

@app.on_event("shutdown")
async def shutdown_event():
    if spark_instance:
        print("Parando SparkSession global...")
        spark_instance.stop()
        print("SparkSession parada.")


# --- Endpoint MODIFICADO com Timeout e Sandboxing ---
@app.post("/execute/pyspark", response_model=ExecutionResponse)
async def execute_pyspark_endpoint(
    request: PySparkCodeRequest,
    background_tasks: BackgroundTasks, # Mantido se precisar de tarefas em background
    spark: SparkSession = Depends(get_spark_session) # Injeta Spark via dependência
):
    """
    Recebe e executa código PySpark em ambiente restrito com timeout.
    Tenta converter o resultado para Pandas (com limite) e retorna JSON.
    """
    try:
        # Executa em threadpool com timeout usando asyncio.wait_for
        task = run_in_threadpool(
            execute_pyspark_code_sync, code_string=request.code, spark_session=spark
        )
        # Espera a conclusão ou timeout
        stdout, stderr, error, new_vars, detected_parquet, detected_view, result_pandas, result_source = await asyncio.wait_for(
            task, timeout=EXECUTION_TIMEOUT_SECONDS
        )

    except asyncio.TimeoutError:
        # O timeout ocorreu
        print(f"ERRO: Execução excedeu o tempo limite de {EXECUTION_TIMEOUT_SECONDS} segundos.")
        # TODO: Tentar cancelar a tarefa Spark correspondente? (Complexo)
        # SparkContext.cancelJobGroup() poderia ser usado se definíssemos um job group.
        # Por agora, apenas retornamos o erro de timeout. A tarefa no threadpool
        # pode continuar rodando até terminar ou falhar, consumindo recursos.
        return ExecutionResponse(
            status="timeout",
            stdout=None, # Pode não ter stdout/stderr se timeout aconteceu cedo
            stderr=f"Erro: Execução excedeu o tempo limite de {EXECUTION_TIMEOUT_SECONDS} segundos.",
            error_message=f"TimeoutError after {EXECUTION_TIMEOUT_SECONDS}s",
            result_source="timeout",
        )
    except Exception as e:
        # Captura erros inesperados na própria invocação do threadpool/wait_for
        # (diferente dos erros DENTRO da função execute_pyspark_code_sync)
        print(f"ERRO inesperado no framework de execução/timeout: {e}")
        return ExecutionResponse(
            status="error",
            stdout=None,
            stderr=f"Erro no framework de execução: {e}",
            error_message=f"Erro no framework: {e}",
            result_source="framework_error",
        )

    # --- Processamento do Resultado (após execução síncrona no threadpool) ---
    response_status = "success" if error is None else "error"
    # Se já houve timeout ou erro de segurança/memória, o status reflete isso
    if result_source in ["timeout", "security_error", "memory_error", "memory_error_pandas"]:
         response_status = result_source # Usa o status mais específico

    error_msg = str(error) if error else None
    pandas_json_result: Optional[List[Dict[str, Any]]] = None

    # Serialização JSON (apenas se a execução NÃO falhou e houve resultado Pandas)
    # Não serializar se houve erro durante a execução ou leitura/conversão
    if result_pandas is not None and error is None:
         print("Serializando DataFrame Pandas para JSON para a resposta da API...")
         try:
             # Tenta serializar (como antes)
             pandas_json_str = result_pandas.to_json(orient='records', date_format='iso', default_handler=str)
             pandas_json_result = json.loads(pandas_json_str)
             print("Serialização JSON concluída.")
         except Exception as json_error:
             print(f"AVISO: Falha na serialização JSON ({json_error}).", file=sys.stderr)
             # Não tenta fallback manual aqui, apenas reporta o erro.
             # Marca como erro na resposta final.
             response_status = "error" # Ou um status mais específico
             error_msg = f"{error_msg if error_msg else ''} | Falha ao serializar resultado Pandas para JSON: {json_error}".strip()
             stderr = f"{stderr if stderr else ''}\n--- Erro Pós-Conversão (Serialização JSON) ---\n{json_error}"
             result_source = "json_serialization_error"
             pandas_json_result = None # Garante que não será enviado resultado parcial/inválido

    # Prepara a resposta final
    created_vars_repr = {k: safe_repr(v) for k, v in new_vars.items()} if new_vars else None

    return ExecutionResponse(
        status=response_status,
        stdout=stdout,
        stderr=stderr,
        error_message=error_msg,
        result_source=result_source,
        result_data_json=pandas_json_result,
        detected_parquet_path=detected_parquet,
        detected_view_name=detected_view,
        created_variables=created_vars_repr,
    )

# --- Ponto de entrada Uvicorn (sem mudanças) ---
if __name__ == "__main__":
    import uvicorn
    print("Iniciando servidor Uvicorn em http://localhost:8000")
    # Nota: 'workers=1' é importante aqui porque estamos usando uma SparkSession global.
    # Múltiplos workers Uvicorn teriam problemas com o estado compartilhado da SparkSession.
    # Para escalar, seria necessária uma arquitetura diferente (ex: workers separados
    # se comunicando com um driver Spark centralizado ou usando Spark Standalone/YARN/K8s).
    uvicorn.run(app, host="0.0.0.0", port=8000, workers=1)