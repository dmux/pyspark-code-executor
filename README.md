# API Executora de Código PySpark com FastAPI

Esta é uma API construída com FastAPI que permite a execução remota de código PySpark arbitrário. A API inicializa uma sessão Spark local, executa o código fornecido, captura as saídas padrão (stdout) e de erro (stderr), tenta detectar automaticamente um resultado (DataFrame Spark salvo como Parquet, uma View temporária ou uma variável DataFrame), converte o resultado detectado para um DataFrame Pandas e retorna o status da execução, logs e os dados do resultado (serializados como JSON) na resposta da API.

**⚠️ AVISO DE SEGURANÇA EXTREMAMENTE IMPORTANTE ⚠️**

Este código utiliza a função `exec()` do Python para executar o código PySpark fornecido pelo usuário. **Isso representa um RISCO DE SEGURANÇA GRAVE**. Permitir a execução de código arbitrário vindo de uma fonte não confiável (como uma API pública) pode levar a vulnerabilidades sérias, incluindo execução remota de código malicioso no servidor onde a API está rodando. **Use este código APENAS em ambientes controlados e confiáveis, ou implemente medidas de segurança robustas (como sandboxing ou validação estrita do código) antes de expô-lo.**

## Funcionalidades

*   **Execução de PySpark via API:** Envie código PySpark através de uma requisição POST.
*   **Inicialização de Spark:** Gerencia uma `SparkSession` local (`local[*]`).
*   **Captura de Saída:** Captura `stdout` e `stderr` da execução do código.
*   **Detecção de Resultado:** Tenta encontrar o resultado da execução através de:
    1.  Um arquivo Parquet salvo no diretório padrão (`/tmp/spark_output/result.parquet` ou o primeiro subdiretório encontrado).
    2.  Uma nova View temporária criada pelo código.
    3.  A última variável criada no escopo de execução que seja um Spark DataFrame.
*   **Conversão para Pandas:** Se um resultado (DataFrame Spark) for detectado, ele é convertido para um DataFrame Pandas usando `.toPandas()`.
*   **Resposta JSON:** Retorna um JSON detalhado com:
    *   Status da execução (`success` ou `error`).
    *   `stdout` e `stderr` capturados.
    *   Mensagem de erro (se houver).
    *   Fonte do resultado detectado (`parquet`, `view`, `direct_dataframe`, `none`, `read_error`, `json_serialization_error`).
    *   Os dados do DataFrame resultante, serializados como uma lista de dicionários JSON (`result_data_json`).
    *   Caminho do Parquet ou nome da View detectados.
    *   Representação das variáveis criadas pelo código (para debug).
*   **Gerenciamento do Ciclo de Vida:** Inicia o Spark no startup da API e o para no shutdown.
*   **Limpeza:** Remove o diretório de saída Parquet anterior antes de cada execução.

## Como Funciona

1.  A API recebe uma requisição POST no endpoint `/execute/pyspark` com um JSON contendo o campo `code` (string PySpark).
2.  A função `execute_pyspark_endpoint` recebe a requisição.
3.  A execução do código PySpark é delegada para a função `execute_pyspark_code_sync` usando `run_in_threadpool` para não bloquear o loop de eventos do FastAPI.
4.  `execute_pyspark_code_sync`:
    *   Limpa o diretório de saída padrão (`/tmp/spark_output`).
    *   Redireciona `stdout` e `stderr` para capturá-los.
    *   Executa o `code_string` usando `exec()`, passando a `SparkSession` (`spark`) como variável local.
    *   **(RISCO DE SEGURANÇA AQUI)**
    *   Restaura `stdout` e `stderr`.
    *   Verifica se a execução gerou exceções.
    *   Se não houve erro na execução, tenta detectar o resultado na ordem: Parquet -> View -> Variável DataFrame.
    *   Se um Spark DataFrame for encontrado, tenta convertê-lo para Pandas usando `.toPandas()`. **(Pode consumir muita memória!)**
    *   Retorna `stdout`, `stderr`, exceção (se houver), variáveis criadas, informações de detecção, o DataFrame Pandas resultante (ou `None`) e a fonte do resultado.
5.  O endpoint da API recebe os resultados da função síncrona.
6.  Se um DataFrame Pandas foi retornado com sucesso, ele é serializado para JSON (`orient='records'`). É incluído um fallback para serialização manual caso a padrão falhe.
7.  Uma resposta `ExecutionResponse` (modelo Pydantic) é construída com todos os detalhes e retornada ao cliente.

## Endpoint da API

### `POST /execute/pyspark`

Executa o código PySpark fornecido.

**Request Body:**

```json
{
  "code": "df = spark.createDataFrame([(1, 'a'), (2, 'b')], ['id', 'value'])\ndf.show()\ndf.write.parquet('/tmp/spark_output/result.parquet', mode='overwrite') # Ou cria uma view, ou deixa 'df' como última variável"
}
```

**Response Body (Exemplo de Sucesso com Parquet):**

{
  "status": "success",
  "stdout": "+---+-----+\n| id|value|\n+---+-----+\n|  1|    a|\n|  2|    b|\n+---+-----+\n\n",
  "stderr": null, // Ou logs do Spark
  "error_message": null,
  "result_source": "parquet",
  "result_data_json": [
    { "id": 1, "value": "a" },
    { "id": 2, "value": "b" }
  ],
  "detected_parquet_path": "/tmp/spark_output/result.parquet",
  "detected_view_name": null,
  "created_variables": {
     "df": "DataFrame[id: bigint, value: string]"
  }
}

**Response Body (Exemplo de Erro):**

{
  "status": "error",
  "stdout": "...", // Pode ter alguma saída antes do erro
  "stderr": "... PySparkException: ...\n", // Detalhes do erro
  "error_message": "PySparkException: ...",
  "result_source": "none", // Ou outra fonte se o erro ocorreu após a detecção/conversão
  "result_data_json": null,
  "detected_parquet_path": null,
  "detected_view_name": null,
  "created_variables": null // Ou as variáveis criadas antes do erro
}