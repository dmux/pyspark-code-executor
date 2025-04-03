import re

def extrair_nome_base_limpo(caminho_arquivo):
  """
  Extrai o nome base limpo de um caminho de arquivo usando regex.

  Remove o caminho (tudo antes da última '/') e a extensão
  (tudo a partir do último '.'). Adicionalmente, substitui quaisquer
  pontos '.' remanescentes no nome base por underscores '_'.
  Funciona com diferentes padrões de caminho.

  Args:
    caminho_arquivo (str): A string contendo o caminho do arquivo.
                             Ex: "s3://bucket/tabela_padrao.parquet",
                                 "{work_space}/F8.TBLR.data",
                                 "arquivo_local.csv"

  Returns:
    str or None: O nome base limpo e modificado do arquivo (ex: "tabela_padrao", "F8_TBLR")
                 ou None se o padrão (nome + extensão) não for encontrado.
  """
  if not caminho_arquivo:
      return None

  # Regex Modificada:
  # (?:.*/)? : Opcionalmente ignora o caminho até a última barra.
  # (.+)     : Captura (grupo 1) um ou mais de QUALQUER caractere.
  #            Isso captura o nome base, INCLUINDO pontos internos, até
  #            o último ponto que precede a extensão.
  # \.       : Corresponde ao ÚLTIMO ponto literal antes da extensão.
  # [^/]*$   : Corresponde à extensão (qualquer coisa que não seja barra até o fim).
  #            O '$' garante que estamos olhando para o final da string.

  padrao = r"(?:.*/)?(.+)\.[^/]*$" # Regex ajustada para capturar pontos no nome
  match = re.search(padrao, caminho_arquivo)

  if match:
    # Pega o nome base bruto (pode conter pontos)
    nome_base_bruto = match.group(1)
    # Substitui todos os pontos por underscores
    nome_base_final = nome_base_bruto.replace('.', '_')
    # Retorna o nome base modificado
    return nome_base_final
  else:
    # Se não houver correspondência (ex: string sem ponto/extensão), retorna None
    # Isso também cobre casos como "pasta/outra_pasta/" que não têm nome.extensão
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
    "arquivo.com.multiplos.pontos.zip", # Deve pegar "arquivo_com_multiplos_pontos"
    ".arquivo_oculto.ini", # Deve pegar "_arquivo_oculto"
    "dados/entrada/F8.TBLR.data", # <-- Caso específico solicitado
    "outros_dados/ARQ.TIPO_X.VERSAO_1.bkp", # <-- Outro caso com pontos
    "arquivos/F8_TBLR_ja_existe.log", # <-- Caso com underscore já existente
    "apenas_um_arquivo", # Sem extensão -> None
    "pasta/outra_pasta/", # Sem nome de arquivo -> None
    "", # String vazia -> None
    None # Entrada None -> None
]

print("--- Testes ---")
for texto in strings_teste:
  nome_limpo = extrair_nome_base_limpo(texto)
  print(f"Entrada: '{texto}' -> Nome Limpo: '{nome_limpo}'")

# Exemplo com uma variável dinâmica
caminho_dinamico = "s3://meu-data-lake/vendas/dia_2023_12_25.parquet"
nome_base = extrair_nome_base_limpo(caminho_dinamico)
print(f"\nCaminho dinâmico: '{caminho_dinamico}' -> Nome Base: '{nome_base}'") # Deve imprimir 'dia_2023_12_25'

caminho_com_ponto = "s3://relatorios/mensal/VENDAS.JULHO.2024.csv"
nome_base_ponto = extrair_nome_base_limpo(caminho_com_ponto)
print(f"Caminho com ponto: '{caminho_com_ponto}' -> Nome Base: '{nome_base_ponto}'") # Deve imprimir 'VENDAS_JULHO_2024'

caminho_f8 = "{work_space}/dados_brutos/F8.QUERY_FOR_TBF8DPR5_DIARIO.parquet"
nome_base_f8 = extrair_nome_base_limpo(caminho_f8)
print(f"Caminho F8: '{caminho_f8}' -> Nome Base: '{nome_base_f8}'") # Deve imprimir 'F8_TBLR'