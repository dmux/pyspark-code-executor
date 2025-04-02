import os
import sys
import io
import re
import json
import shutil
from pathlib import Path
from typing import Optional, Any, List, Dict, Tuple

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel, Field
import pandas as pd

# --- Imports e Inicialização do Spark (sem mudanças) ---
try:
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
    from pyspark.errors import PySparkException

    print("Inicializando SparkSession global...")
    spark = SparkSession.builder \
        .appName("FastAPI PySpark Executor") \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.legacy.createHiveTableByDefault", "false") \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .getOrCreate()
    print("SparkSession inicializada.")
    SC = spark.sparkContext
    print(f"Spark UI disponível em: {SC.uiWebUrl}")

except ImportError:
    print("ERRO: PySpark não parece estar instalado ou configurável.")
    spark = None
    PySparkException = Exception
    SparkDataFrame = None
except Exception as e:
    print(f"ERRO ao inicializar SparkSession: {e}")
    spark = None
    PySparkException = Exception
    SparkDataFrame = None

# --- Constantes (sem mudanças) ---
DEFAULT_PARQUET_OUTPUT_DIR = Path("/tmp/spark_output")
DEFAULT_PARQUET_OUTPUT_PATH = DEFAULT_PARQUET_OUTPUT_DIR / "result.parquet"

# --- Modelos Pydantic (sem mudanças) ---
class PySparkCodeRequest(BaseModel):
    code: str = Field(..., description="O código PySpark a ser executado.")

class ExecutionResponse(BaseModel):
    status: str = Field(description="Status da execução ('success' ou 'error').")
    stdout: Optional[str] = Field(None, description="Saída padrão capturada da execução.")
    stderr: Optional[str] = Field(None, description="Saída de erro capturada da execução.")
    error_message: Optional[str] = Field(None, description="Mensagem de erro específica se a execução falhou.")
    result_source: Optional[str] = Field(None, description="Origem do resultado ('parquet', 'view', 'direct_dataframe', 'none', 'read_error').")
    # A resposta JSON ainda conterá a *representação* JSON do DataFrame
    result_data_json: Optional[List[Dict[str, Any]]] = Field(None, description="Dados do resultado (DataFrame completo convertido para JSON), como uma lista de dicionários.")
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


# --- Lógica de Execução MODIFICADA ---
def execute_pyspark_code_sync(
    code_string: str, spark_session: SparkSession
) -> Tuple[str, str, Optional[Exception], Dict[str, Any], Optional[str], Optional[str], Optional[pd.DataFrame], str]:
    """
    Executa código PySpark SÍNCRONAMENTE, captura saídas, detecta resultados,
    e TENTA converter o resultado (se for DataFrame) para Pandas.

    Retorna: (
        stdout,
        stderr,
        execution_exception, # Erro durante o exec()
        new_vars_dict,       # Dicionário com as variáveis criadas pelo exec
        detected_parquet,    # Path do parquet detectado
        detected_view,       # Nome da view detectada
        result_pandas_df,    # DataFrame Pandas resultante (ou None)
        result_source        # String indicando a origem ('parquet', 'view', 'direct', 'none', 'read_error')
    )
    """
    if not spark_session:
        return "", "SparkSession não está disponível.", RuntimeError("SparkSession not initialized"), {}, None, None, None, "error"

    print("--- Código PySpark a ser executado ---")
    print(code_string)
    print("--------------------------------------")
    cleanup_output()

    old_stdout = sys.stdout
    old_stderr = sys.stderr
    redirected_output = io.StringIO()
    redirected_error = io.StringIO()
    sys.stdout = redirected_output
    sys.stderr = redirected_error

    exec_locals = {'spark': spark_session, '__builtins__': __builtins__}
    initial_keys = set(exec_locals.keys())
    initial_views = set(t.name for t in spark_session.catalog.listTables() if t.isTemporary)

    execution_exception: Optional[Exception] = None
    new_vars_dict: Dict[str, Any] = {}
    detected_parquet_path_str: Optional[str] = None
    detected_view_name: Optional[str] = None
    result_pandas_df: Optional[pd.DataFrame] = None
    result_source: str = "none" # Começa como 'none'

    try:
        # ATENÇÃO: RISCO DE SEGURANÇA!!!
        exec(code_string, {}, exec_locals)

    except PySparkException as e:
        print(f"\nErro PySpark durante a execução: {e}", file=sys.stderr)
        execution_exception = e
    except Exception as e:
        print(f"\nErro Python durante a execução: {e}", file=sys.stderr)
        execution_exception = e
    # O finally garante que stdout/stderr sejam restaurados, mas a lógica de
    # leitura/conversão acontece *depois* do try/except do exec.

    # Restaura stdout/stderr ANTES de tentar ler/converter,
    # para que os logs dessas operações apareçam no console normal.
    sys.stdout = old_stdout
    sys.stderr = old_stderr
    stdout_output = redirected_output.getvalue() # Pega o valor *antes* de restaurar
    stderr_output = redirected_error.getvalue()  # Pega o valor *antes* de restaurar

    # Captura Novas Variáveis independentemente de erro na execução
    final_keys = set(exec_locals.keys())
    new_keys = final_keys - initial_keys
    if new_keys:
        print(f"Variáveis criadas pela execução: {', '.join(new_keys)}")
        new_vars_dict = {key: exec_locals[key] for key in new_keys}


    # --- Tentativa de Leitura e Conversão PÓS-EXECUÇÃO (se não houve erro no exec) ---
    if execution_exception is None:
        spark_df_to_convert: Optional[SparkDataFrame] = None
        read_or_convert_error: Optional[Exception] = None

        try:
            # 1. Detecção de Parquet
            if DEFAULT_PARQUET_OUTPUT_PATH.exists() or any(DEFAULT_PARQUET_OUTPUT_DIR.iterdir()):
                 potential_path = next(DEFAULT_PARQUET_OUTPUT_DIR.iterdir(), None)
                 if DEFAULT_PARQUET_OUTPUT_PATH.exists():
                     detected_parquet_path_str = str(DEFAULT_PARQUET_OUTPUT_PATH.resolve())
                 elif potential_path:
                     detected_parquet_path_str = str(potential_path.resolve())

                 if detected_parquet_path_str:
                    print(f"Detectado Parquet em: {detected_parquet_path_str}. Tentando ler...")
                    spark_df_to_convert = spark_session.read.parquet(detected_parquet_path_str)
                    result_source = "parquet"

            # 2. Detecção de View (só se não achou parquet)
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

            # 3. Detecção de DataFrame Direto (só se não achou parquet/view)
            if spark_df_to_convert is None and new_vars_dict:
                 print("Verificando variáveis criadas por um DataFrame Spark...")
                 last_var_name = list(new_vars_dict.keys())[-1] # Heurística
                 last_var_value = new_vars_dict[last_var_name]
                 if SparkDataFrame and isinstance(last_var_value, SparkDataFrame):
                     print(f"Encontrado Spark DataFrame na variável: '{last_var_name}'")
                     spark_df_to_convert = last_var_value
                     result_source = "direct_dataframe"
                 else:
                      print("Última variável não era um Spark DataFrame ou não houve variáveis novas.")


            # 4. Conversão para Pandas (se um DF Spark foi encontrado)
            if spark_df_to_convert is not None:
                print(f"Convertendo DataFrame Spark (origem: {result_source}) para Pandas...")
                # ------ AVISO DE MEMÓRIA: .toPandas() ------
                result_pandas_df = spark_df_to_convert.toPandas()
                # ------------------------------------------
                print(f"Conversão para Pandas concluída. Shape: {result_pandas_df.shape}")
            elif result_source == "none":
                 print("Nenhum resultado (Parquet, View, DataFrame direto) detectado para conversão.")


        except PySparkException as e:
            print(f"\nErro PySpark ao LER/CONVERTER resultado: {e}", file=sys.stderr)
            stderr_output += f"\n--- Erro Pós-Execução (Leitura/Conversão) ---\n{e}"
            read_or_convert_error = e
            result_source = "read_error" # Indica que houve erro na leitura/conversão
        except Exception as e:
            print(f"\nErro Python ao LER/CONVERTER resultado: {e}", file=sys.stderr)
            stderr_output += f"\n--- Erro Pós-Execução (Leitura/Conversão) ---\n{e}"
            read_or_convert_error = e
            result_source = "read_error"

        # Se houve erro na leitura/conversão, anexamos ao erro da execução (se houver)
        # ou o tornamos o erro principal se a execução foi OK.
        if read_or_convert_error and not execution_exception:
             execution_exception = read_or_convert_error


    print("--- Fim da Execução Síncrona ---")

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
    title="PySpark Code Executor API",
    description="...", # Manter descrição com aviso de segurança
    version="0.1.0",
)

# --- Eventos startup/shutdown (sem mudanças) ---
@app.on_event("startup")
async def startup_event():
    if spark is None:
        print("AVISO: SparkSession não foi inicializada.")
    DEFAULT_PARQUET_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

@app.on_event("shutdown")
async def shutdown_event():
    if spark:
        print("Parando SparkSession global...")
        spark.stop()
        print("SparkSession parada.")


# --- Endpoint MODIFICADO ---
@app.post("/execute/pyspark", response_model=ExecutionResponse)
async def execute_pyspark_endpoint(request: PySparkCodeRequest, background_tasks: BackgroundTasks):
    """
    Recebe e executa código PySpark. Tenta converter o resultado para Pandas
    e retorna a representação JSON na resposta.
    """
    if not spark:
        raise HTTPException(status_code=503, detail="SparkSession não está disponível.")

    try:
        # Executa em threadpool, recebe o objeto Pandas diretamente
        stdout, stderr, error, new_vars, detected_parquet, detected_view, result_pandas, result_source = await run_in_threadpool(
            execute_pyspark_code_sync, code_string=request.code, spark_session=spark
        )
    except Exception as e:
        # Captura erros inesperados na própria execução do threadpool
        return ExecutionResponse(
            status="error",
            stdout=None,
            stderr=f"Erro ao invocar a execução: {e}",
            error_message=f"Erro no framework de execução: {e}",
            result_source="framework_error", # Novo tipo de erro
        )

    response_status = "success" if error is None else "error"
    error_msg = str(error) if error else None
    pandas_json_result: Optional[List[Dict[str, Any]]] = None

    # Agora, se result_pandas foi retornado, serializamos para JSON aqui
    if result_pandas is not None and error is None: # Só serializa se teve resultado E não houve erro geral
         print("Serializando DataFrame Pandas para JSON para a resposta da API...")
         try:
             # Tenta serializar o objeto Pandas que recebemos
             pandas_json_str = result_pandas.to_json(orient='records', date_format='iso', default_handler=str)
             pandas_json_result = json.loads(pandas_json_str)
             print("Serialização JSON concluída.")
         except Exception as json_error:
             print(f"AVISO: Falha na serialização JSON ({json_error}). Tentando fallback manual...")
             # Fallback (como antes)
             try:
                pandas_json_result = result_pandas.apply(lambda row: {col: str(row[col]) if not pd.isna(row[col]) else None for col in result_pandas.columns}, axis=1).tolist()
                print("Serialização JSON manual concluída.")
             except Exception as manual_json_error:
                 print(f"ERRO: Falha na serialização manual para JSON: {manual_json_error}", file=sys.stderr)
                 response_status = "error" # Marca erro se a serialização falhar
                 error_msg = f"Execução/Conversão OK, mas falha ao serializar Pandas para JSON: {manual_json_error}"
                 stderr += f"\n--- Erro Pós-Conversão (Serialização JSON) ---\n{error_msg}"
                 result_source = "json_serialization_error"


    # Prepara a resposta final
    created_vars_repr = {k: safe_repr(v) for k, v in new_vars.items()} if new_vars else None

    # Note que usamos 'result_data_json' no modelo de resposta
    return ExecutionResponse(
        status=response_status,
        stdout=stdout,
        stderr=stderr,
        error_message=error_msg,
        result_source=result_source, # Usamos a string retornada pela função
        result_data_json=pandas_json_result, # Enviamos o JSON serializado
        detected_parquet_path=detected_parquet,
        detected_view_name=detected_view,
        created_variables=created_vars_repr,
    )

# --- Ponto de entrada Uvicorn (sem mudanças) ---
if __name__ == "__main__":
    import uvicorn
    print("Iniciando servidor Uvicorn em http://localhost:8000")
    uvicorn.run(app, host="0.0.0.0", port=8000, workers=1)