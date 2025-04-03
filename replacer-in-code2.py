import re
import os # Necessário para o splitext no pós-processamento

class ExemploClasse: # Apenas para dar contexto ao 'self'
    def __init__(self):
        # As regras poderiam ser definidas aqui, ex: self.regras_padrao = [...]
        pass

    def _extrair_nome_base_limpo(self, caminho_arquivo, regras):
        """
        Extrai o nome base limpo de um caminho de arquivo usando uma lista de regras regex.

        Tenta cada padrão regex na lista 'regras'. Para a primeira correspondência,
        extrai o grupo 1, remove a extensão (se capturada) e substitui
        pontos remanescentes por underscores.

        Args:
            caminho_arquivo (str): A string contendo o caminho do arquivo.
            regras (list[str]): Uma lista de strings contendo padrões regex.
                                Cada regex deve idealmente capturar a parte
                                desejada do nome no grupo 1.

        Returns:
            str or None: O nome base limpo e modificado do arquivo, ou None se
                         nenhuma regra corresponder ou o caminho for inválido.
        """
        if not caminho_arquivo or not regras:
            return None

        base_bruta = None
        for padrao in regras:
            try:
                match = re.search(padrao, caminho_arquivo)
                if match:
                    # Assume que o grupo 1 contém a parte relevante
                    # Pode precisar de tratamento de erro se match.groups() for vazio
                    if match.groups(): # Verifica se há grupos de captura
                        grupo_capturado = match.group(1)
                        # Validação básica do que foi capturado
                        if grupo_capturado and grupo_capturado not in ('.', '..'):
                            base_bruta = grupo_capturado
                            # Encontrou uma correspondência válida, para de tentar outras regras
                            break
                        # Se capturou algo inválido (como '.'), continua para a próxima regra
                    else:
                        # A regex casou, mas não tinha grupo de captura. Isso pode ser um erro na regra.
                        # Ou talvez a intenção era usar group(0)? Vamos ignorar por enquanto.
                        # print(f"Aviso: Regra '{padrao}' correspondeu mas não tem grupo de captura.")
                        pass # Continua para a próxima regra

            except re.error as e:
                # Lidar com uma regex inválida na lista de regras
                print(f"Erro na regex da regra '{padrao}': {e}")
                continue # Pula para a próxima regra
            except IndexError:
                 # Caso raro onde group(1) não existe mesmo após verificar match.groups()
                 # print(f"Aviso: IndexError ao acessar group(1) para regra '{padrao}'.")
                 continue # Pula para a próxima regra


        # --- Pós-processamento (aplicado SE uma regra encontrou algo) ---
        if base_bruta:
            # 1. Remove a extensão da 'base_bruta' capturada, caso a regex
            #    tenha incluído a extensão (ex: se a regra era r"(?:.*/)?([^/]+)$")
            name_part, ext_part = os.path.splitext(base_bruta)

            # 2. Determina a string base real para limpeza
            if not name_part and ext_part: # Lida com casos como ".bashrc" capturado
                base_para_processar = ext_part
            else:
                 # Usa name_part se existir, senão usa a base_bruta inteira (caso sem extensão)
                base_para_processar = name_part if name_part else base_bruta

            if not base_para_processar: # Verificação final
                return None

            # 3. Substitui pontos por underscores
            nome_limpo = base_para_processar.replace('.', '_')
            return nome_limpo
        else:
            # Nenhuma regra correspondeu ou produziu um resultado válido
            return None

# --- Exemplo de Uso ---

processador = ExemploClasse()

# Definindo diferentes conjuntos de regras
regras_originais = [
    r"(?:.*/)?([^.]+)\.[^/]*$" # Exige extensão, pega antes do último ponto
]

regras_flexiveis = [
    # Tenta primeiro pegar algo específico como F8.TBLR antes da extensão
    r"(?:.*/)?(F8\.TBLR)\.[^/]*$",
    # Tenta pegar qualquer nome antes da extensão (pode incluir pontos)
    r"(?:.*/)?(.+)\.[^/]*$",
    # Se falhar (sem extensão?), pega tudo depois da última barra
    r"(?:.*/)?([^/]+)$"
]

regras_so_basename = [
    # Apenas captura o nome completo do arquivo (incluindo extensão)
    # O pós-processamento fará a separação e limpeza.
    r"(?:.*/)?([^/]+)$"
]


strings_teste = [
    "s3://bucket/tabela_padrao.parquet",
    "{work_space}/F8.TBLR.data",
    "dados/relatorio.v2.csv",
    "arquivo_sem_extensao",
    "/path/to/.config",
    "pasta/",
    "",
    None,
    "C:\\windows\\system.ini",
    "docs/README", # Sem extensão, capturado pela regra flexível ou só basename
    "images/logo.jpeg",
    "special_reports/F8.TBLR_final.log" # O ponto F8.TBLR NÃO será substituído se a regra for específica
]

print("--- Teste com Regras Originais (Exige Extensão) ---")
for texto in strings_teste:
    nome = processador._extrair_nome_base_limpo(texto, regras_originais)
    print(f"Entrada: '{texto}' -> Nome Limpo: '{nome}'")
# Esperado: Funciona para com extensão, None para sem extensão. F8.TBLR -> F8_TBLR

print("\n--- Teste com Regras Flexíveis ---")
for texto in strings_teste:
    nome = processador._extrair_nome_base_limpo(texto, regras_flexiveis)
    print(f"Entrada: '{texto}' -> Nome Limpo: '{nome}'")
# Esperado: Funciona para a maioria. F8.TBLR.data -> F8_TBLR (regra específica ou segunda regra). arquivo_sem_extensao -> arquivo_sem_extensao (terceira regra).

print("\n--- Teste com Regra Só Basename + Pós-processamento ---")
for texto in strings_teste:
    nome = processador._extrair_nome_base_limpo(texto, regras_so_basename)
    print(f"Entrada: '{texto}' -> Nome Limpo: '{nome}'")
# Esperado: Funciona para a maioria, pois depende do pós-processamento robusto. F8.TBLR.data -> F8_TBLR. arquivo_sem_extensao -> arquivo_sem_extensao.