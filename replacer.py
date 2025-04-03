import re

def extrair_nome_base_limpo(caminho_arquivo):
  """
  Extrai o nome base limpo de um caminho de arquivo usando regex.

  Remove o caminho (tudo antes da última '/') e a extensão
  (tudo a partir do último '.'). Funciona com diferentes padrões de caminho.

  Args:
    caminho_arquivo (str): A string contendo o caminho do arquivo.
                             Ex: "s3://bucket/tabela_padrao.parquet",
                                 "{work_space}/porcetagem_correta.parquet",
                                 "arquivo_local.csv"

  Returns:
    str or None: O nome base limpo do arquivo (ex: "tabela_padrao")
                 ou None se o padrão (nome + extensão) não for encontrado.
  """
  if not caminho_arquivo:
      return None

  # Regex:
  # (?:.*/)? : Opcionalmente (?), combina qualquer caractere (.*) seguido por uma barra (/),
  #             em um grupo não-capturante (?:...). Isso ignora o caminho.
  #             O '.*' é "guloso" (greedy), então vai até a última barra.
  # ([^./]+) : Captura (grupo 1) um ou mais caracteres (+) que NÃO são (.)
  #             uma barra (/) nem um ponto (\.). Isso captura o nome base desejado.
  #             Importante incluir a barra aqui para evitar capturar parte do caminho
  #             se não houver ponto (embora o próximo passo deva evitar isso).
  # (?:\.[^/]*)$ : Combina um grupo não-capturante (?:...) que começa com um ponto literal (\.)
  #              seguido por zero ou mais caracteres (*) que NÃO são uma barra ([^/]).
  #              Isso representa a extensão. O $ garante que isso ocorra no final da string.

  # Regex Alternativa (mais simples, focando no último ponto e última barra):
  # (?:.*/)?   : Opcionalmente ignora o caminho até a última barra.
  # ([^.]+)    : Captura (grupo 1) um ou mais caracteres que NÃO são um ponto.
  #              Este é o nome base.
  # \.         : Corresponde ao ponto literal antes da extensão.
  # [^/]*$     : Corresponde à extensão (qualquer coisa que não seja barra até o fim).
  #              Esta versão é geralmente suficiente e mais fácil de ler.

  padrao = r"(?:.*/)?([^.]+)\.[^/]*$"
  padrao = r"(?:.*/)?(.+)\.[^/]*$" # Regex ajustada para capturar pontos no nome

  match = re.search(padrao, caminho_arquivo)

  if match:
    # Retorna o conteúdo do primeiro grupo de captura (o nome base)
    return match.group(1)
  else:
    # Se não houver correspondência (ex: string sem ponto/extensão), retorna None
    return None

# --- Exemplos de Uso ---
strings_teste = [
    "s3://bucket/tabela_padrao.parquet",
    "{work_space}/porcetagem_correta.parquet",
    "outro_bucket/subpasta/arquivo_final.csv",
    "arquivo_local.txt",
    "/caminho/absoluto/no/linux/script.sh",
    "C:\\caminho\\windows\\relatorio.docx", # Regex funciona com barras invertidas também
    "nome_sem_caminho.json",
    "arquivo.com.multiplos.pontos.zip", # Deve pegar "arquivo.com.multiplos.pontos"
    ".arquivo_oculto.ini", # Deve pegar ".arquivo_oculto"
    "apenas_um_arquivo", # Sem extensão -> None
    "pasta/outra_pasta/", # Sem nome de arquivo -> None
    "", # String vazia -> None
    None # Entrada None -> None
]

for texto in strings_teste:
  nome_limpo = extrair_nome_base_limpo(texto)
  print(f"Entrada: '{texto}' -> Nome Limpo: '{nome_limpo}'")

# Exemplo com uma variável dinâmica
caminho_dinamico = "s3://meu-data-lake/vendas/dia_2023_12_25.parquet"
nome_base = extrair_nome_base_limpo(caminho_dinamico)
print(f"\nCaminho dinâmico: '{caminho_dinamico}' -> Nome Base: '{nome_base}'") # Deve imprimir 'dia_2023_12_25'