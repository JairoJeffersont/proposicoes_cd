import sys
import os
from tqdm import tqdm  
from database.database import criar_conexao, fechar_conexao

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from middleware.getJson import getJson

def buscarProposicoes(ano):
    dados = getJson(f"https://dadosabertos.camara.leg.br/arquivos/proposicoes/json/proposicoes-{ano}.json")
    
    if not dados or "dados" not in dados:
        print("JSON inválido ou sem dados disponíveis.")
        return

    return dados["dados"]
   
def inserirProposicoes(ano):
    proposicoes = buscarProposicoes(ano)
    total_proposicoes = len(proposicoes)
    conexao = criar_conexao()
    if not conexao:
        print("Erro ao criar conexão com o banco de dados.\n")
        return
    
    try:
        apagar = conexao.cursor();
        apagar.execute(f'DELETE FROM proposicoes WHERE proposicao_ano = {ano}');
        
        cursor = conexao.cursor()
        for index, proposicao in enumerate(tqdm(proposicoes, desc="Inserindo proposições", bar_format="\033[1;34m{l_bar}{bar}{r_bar}\033[0m")):
            proposicao_id = proposicao["id"]
            numero = proposicao["numero"]
            ano = proposicao["ano"] if proposicao["ano"] != 0 else ano
            sigla_tipo = proposicao["siglaTipo"]
            ementa = proposicao["ementa"]
            data_apresentacao = proposicao["dataApresentacao"]
            url = proposicao["uriPropPrincipal"]
            
            if url:
                principal = url.split('/')[-1]
            else:
                principal = None
            
            id_situacao = proposicao["ultimoStatus"].get("idSituacao", "")
            arquivada = 1 if id_situacao in ["9923", "1140"] else 0
            aprovada = 1 if id_situacao == "1140" else 0
            
            sql = "INSERT INTO proposicoes (proposicao_id, proposicao_numero, proposicao_titulo, proposicao_ano, proposicao_tipo, proposicao_ementa, proposicao_apresentacao, proposicao_arquivada, proposicao_aprovada, proposicao_principal) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE proposicao_titulo = VALUES(proposicao_titulo), proposicao_ano = VALUES(proposicao_ano), proposicao_tipo = VALUES(proposicao_tipo), proposicao_ementa = VALUES(proposicao_ementa), proposicao_apresentacao = VALUES(proposicao_apresentacao), proposicao_arquivada = VALUES(proposicao_arquivada), proposicao_aprovada = VALUES(proposicao_aprovada), proposicao_principal = VALUES(proposicao_principal)"

            valores = (proposicao_id, numero, f"{sigla_tipo} {numero}/{ano}", ano, sigla_tipo, ementa, data_apresentacao, arquivada, aprovada, principal)
            cursor.execute(sql, valores)
            conexao.commit()
        print(f"\nTodas as {total_proposicoes} proposições do ano {ano} foram inseridas com sucesso!\n")
    except Exception as e:
        print(f"Erro ao inserir proposições: {e}")
        conexao.rollback()
    finally:
        fechar_conexao(conexao)

def buscarAutoresProposicoes(ano):
    dados = getJson(f"https://dadosabertos.camara.leg.br/arquivos/proposicoesAutores/json/proposicoesAutores-{ano}.json")
    
    if not dados or "dados" not in dados:
        print("JSON inválido ou sem dados disponíveis.")
        return

    return dados["dados"]

def inserirAutoresProposicoes(ano):
    proposicao_autor = buscarAutoresProposicoes(ano)
    total_proposicoes = len(proposicao_autor)
    conexao = criar_conexao()
    if not conexao:
        print("Erro ao criar conexão com o banco de dados.\n")
        return
    
    try:
        apagar = conexao.cursor()
        apagar.execute(f'DELETE FROM proposicoes_autores WHERE proposicao_autor_ano = {ano}')
        cursor = conexao.cursor()
        
        for index, proposicao in enumerate(tqdm(proposicao_autor, desc="Inserindo proposições", bar_format="\033[1;31m{l_bar}{bar}{r_bar}\033[0m")):
            proposicao_id = proposicao.get("idProposicao", 0)  # Substituir por 0 se não existir
            autor_id = proposicao.get("idDeputadoAutor", 0)  # Substituir por 0 se não existir
            autor_nome = proposicao.get("nomeAutor", "")  # Substituir por string vazia se não existir
            autor_partido = proposicao.get("siglaPartidoAutor", "")  # Substituir por string vazia se não existir
            autor_estado = proposicao.get("siglaUFAutor", "")  # Substituir por string vazia se não existir
            proponente = proposicao.get("proponente", 0)  # Substituir por 0 se não existir
            ordem_assinatura = proposicao.get("ordemAssinatura", 0)  # Substituir por 0 se não existir
            
            # Inserir ano com base no ano passado
            autor_ano = ano

            # Se algum campo importante estiver ausente, o valor será substituído por 0
            autor_id = autor_id if autor_id != 0 else 0
            autor_nome = autor_nome if autor_nome != "" else ""
            autor_partido = autor_partido if autor_partido != "" else ""
            autor_estado = autor_estado if autor_estado != "" else ""
            proponente = proponente if proponente != 0 else 0
            ordem_assinatura = ordem_assinatura if ordem_assinatura != 0 else 0

            # Comando SQL para inserir os dados
            sql = "INSERT INTO proposicoes_autores (proposicao_id, proposicao_autor_id, proposicao_autor_nome, proposicao_autor_partido, proposicao_autor_estado, proposicao_autor_proponente, proposicao_autor_assinatura, proposicao_autor_ano) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE proposicao_autor_nome = VALUES(proposicao_autor_nome), proposicao_autor_partido = VALUES(proposicao_autor_partido), proposicao_autor_estado = VALUES(proposicao_autor_estado), proposicao_autor_proponente = VALUES(proposicao_autor_proponente), proposicao_autor_assinatura = VALUES(proposicao_autor_assinatura), proposicao_autor_ano = VALUES(proposicao_autor_ano)"

            valores = (proposicao_id, autor_id, autor_nome, autor_partido, autor_estado, proponente, ordem_assinatura, autor_ano)

            cursor.execute(sql, valores)

        conexao.commit()
        print(f"\nTodas as {total_proposicoes} proposições com autores do ano {ano} foram inseridas com sucesso!\n")
            
    
    except Exception as e:
        print(f"Erro ao inserir proposições: {e}")
        conexao.rollback()
    finally:
        fechar_conexao(conexao)

def criarSqlProposicoes(ano):
    proposicoes = buscarProposicoes(ano)
    total_proposicoes = len(proposicoes)
    
    # Verifica se a pasta /sql existe, caso contrário, cria
    if not os.path.exists("sql"):
        os.makedirs("sql")
    
    # Nome do arquivo onde o SQL será salvo
    arquivo_sql = f"sql/inserir_proposicoes_{ano}.sql"
    
    try:
        with open(arquivo_sql, "w", encoding="utf-8") as file:
            # Escreve o cabeçalho do arquivo SQL
            file.write(f"-- Inserções de proposições do ano {ano}\n")
            file.write(f"-- Gerado automaticamente\n\n")
            
            # Para cada proposição, gera um comando SQL INSERT
            for index, proposicao in enumerate(tqdm(proposicoes, desc="Gerando SQL para proposições")):
                proposicao_id = proposicao["id"]
                numero = proposicao["numero"]
                ano = proposicao["ano"] if proposicao["ano"] != 0 else ano
                sigla_tipo = proposicao["siglaTipo"]
                ementa = proposicao["ementa"]
                ementa = ementa.replace("'", "\\'").replace('"', '\\"')
                data_apresentacao = proposicao["dataApresentacao"]
                url = proposicao["uriPropPrincipal"]
                
                if url:
                    principal = url.split('/')[-1]
                else:
                    principal = 0
                
                id_situacao = proposicao["ultimoStatus"].get("idSituacao", "")
                arquivada = 1 if id_situacao in ["9923", "1140"] else 0
                aprovada = 1 if id_situacao == "1140" else 0

                sql = f"INSERT INTO proposicoes (proposicao_id, proposicao_numero, proposicao_titulo, proposicao_ano, proposicao_tipo, proposicao_ementa, proposicao_apresentacao, proposicao_arquivada, proposicao_aprovada, proposicao_principal) VALUES ({proposicao_id}, {numero}, '{sigla_tipo} {numero}/{ano}', {ano}, '{sigla_tipo}', '{ementa}', '{data_apresentacao}', {arquivada}, {aprovada}, '{principal}');\n"
                
                file.write(sql)

        print(f"\nTodas as {total_proposicoes} proposições do ano {ano} foram processadas e salvas em '{arquivo_sql}'.\n")
    
    except Exception as e:
        print(f"Erro ao gerar o arquivo SQL: {e}")
        
def criarSqlAutores(ano):
    proposicao_autor = buscarAutoresProposicoes(ano)
    total_proposicoes = len(proposicao_autor)
    
    # Verifica se a pasta /sql existe, caso contrário, cria
    if not os.path.exists("sql"):
        os.makedirs("sql")
    
    # Nome do arquivo onde o SQL será salvo
    arquivo_sql = f"sql/inserir_autores_proposicoes_{ano}.sql"
    
    try:
        with open(arquivo_sql, "w", encoding="utf-8") as file:
            # Escreve o cabeçalho do arquivo SQL
            file.write(f"-- Inserções de autores das proposições do ano {ano}\n")
            file.write(f"-- Gerado automaticamente\n\n")
            
            # Para cada proposição, gera um comando SQL INSERT
            for index, proposicao in enumerate(tqdm(proposicao_autor, desc="Gerando SQL para autores")):
                proposicao_id = proposicao.get("idProposicao", 0)  # Substituir por 0 se não existir
                autor_id = proposicao.get("idDeputadoAutor", 0)  # Substituir por 0 se não existir
                autor_nome = proposicao.get("nomeAutor", "")  # Substituir por string vazia se não existir
                autor_partido = proposicao.get("siglaPartidoAutor", "")  # Substituir por string vazia se não existir
                autor_estado = proposicao.get("siglaUFAutor", "")  # Substituir por string vazia se não existir
                proponente = proposicao.get("proponente", 0)  # Substituir por 0 se não existir
                ordem_assinatura = proposicao.get("ordemAssinatura", 0)  # Substituir por 0 se não existir
                
                # Inserir ano com base no ano passado
                autor_ano = ano

                # Se algum campo importante estiver ausente, o valor será substituído por 0
                autor_id = autor_id if autor_id != 0 else 0
                autor_nome = autor_nome if autor_nome != "" else ""
                autor_partido = autor_partido if autor_partido != "" else ""
                autor_estado = autor_estado if autor_estado != "" else ""
                proponente = proponente if proponente != 0 else 0
                ordem_assinatura = ordem_assinatura if ordem_assinatura != 0 else 0

                # Comando SQL de inserção
                sql = f"INSERT INTO proposicoes_autores (proposicao_id, proposicao_autor_id, proposicao_autor_nome, proposicao_autor_partido, proposicao_autor_estado, proposicao_autor_proponente, proposicao_autor_assinatura, proposicao_autor_ano) VALUES ({proposicao_id}, {autor_id}, '{autor_nome}', '{autor_partido}', '{autor_estado}', {proponente}, {ordem_assinatura}, {autor_ano});\n"
                
                # Escreve o comando SQL no arquivo
                file.write(sql)

        print(f"\nTodas as {total_proposicoes} proposições com autores do ano {ano} foram processadas e salvas em '{arquivo_sql}'.\n")
    
    except Exception as e:
        print(f"Erro ao gerar o arquivo SQL: {e}")