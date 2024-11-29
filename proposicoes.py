import requests
import argparse

from datetime import datetime
from tqdm import tqdm  # Para a barra de progresso

from database import criar_conexao, fechar_conexao
from json_handler import obter_json_da_url

def obter_json_da_url(url):
    try:
        print("Conectando à API...")
        resposta = requests.get(url)
        resposta.raise_for_status()
        print("Conexão com a API bem-sucedida.")
        return resposta.json()
    except requests.exceptions.RequestException as e:
        print(f"Erro ao acessar a URL: {e}")
        return None

def inserir_proposicoes(ano):
    url = f"https://dadosabertos.camara.leg.br/arquivos/proposicoes/json/proposicoes-{ano}.json"
    dados = obter_json_da_url(url)
    
    if not dados or "dados" not in dados:
        print("JSON inválido ou sem dados disponíveis.")
        return
    
    proposicoes = dados["dados"]  # Lista de proposições
    total_proposicoes = len(proposicoes)
    print(f"{total_proposicoes} proposições encontradas para o ano {ano}.")

    conexao = criar_conexao()
    if not conexao:
        print("Erro ao criar conexão com o banco de dados.")
        return
    
    try:
        
        apagar = conexao.cursor();
        apagar.execute(f'DELETE FROM proposicoes WHERE proposicao_ano = {ano}');
        
        cursor = conexao.cursor()

        # Barra de progresso para a inserção das proposições
        for index, proposicao in enumerate(tqdm(proposicoes, desc="Inserindo proposições")):
            # Extraindo os campos desejados
            proposicao_id = proposicao["id"]
            numero = proposicao["numero"]
            
            # Corrigir ano: se proposicao["ano"] for 0, usamos o ano passado para a função
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

            # Determinando valores para os campos `proposicao_arquivada` e `proposicao_aprovada`
            arquivada = 1 if id_situacao in ["9923", "1140"] else 0
            aprovada = 1 if id_situacao == "1140" else 0
            
            # Comando SQL para inserir os dados
            sql = """
                INSERT INTO proposicoes (
                    proposicao_id,
                    proposicao_numero,
                    proposicao_titulo,
                    proposicao_ano,
                    proposicao_tipo,
                    proposicao_ementa,
                    proposicao_apresentacao,
                    proposicao_arquivada,
                    proposicao_aprovada,
                    proposicao_principal
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    proposicao_titulo = VALUES(proposicao_titulo),
                    proposicao_ano = VALUES(proposicao_ano),
                    proposicao_tipo = VALUES(proposicao_tipo),
                    proposicao_ementa = VALUES(proposicao_ementa),
                    proposicao_apresentacao = VALUES(proposicao_apresentacao),
                    proposicao_arquivada = VALUES(proposicao_arquivada),
                    proposicao_aprovada = VALUES(proposicao_aprovada),
                    proposicao_principal = VALUES(proposicao_principal)
            """
            valores = (
                proposicao_id,
                numero,
                f"{sigla_tipo} {numero}/{ano}",
                ano,
                sigla_tipo,
                ementa,
                data_apresentacao,
                arquivada,
                aprovada,
                principal
            )

            cursor.execute(sql, valores)

        conexao.commit()
        print(f"Todas as {total_proposicoes} proposições do ano {ano} foram inseridas com sucesso!")
    except Exception as e:
        print(f"Erro ao inserir proposições: {e}")
        conexao.rollback()
    finally:
        fechar_conexao(conexao)
        print("Conexão com o banco de dados encerrada.")

if __name__ == "__main__":
    # Configuração do argparse para aceitar o argumento -ano
    parser = argparse.ArgumentParser(description="Processamento de proposições.")
    parser.add_argument("-ano", type=int, default=0, help="Ano para inserir proposições (0 para todos os anos desde 1950).")
    args = parser.parse_args()

    ano_atual = datetime.now().year
    ano_especifico = args.ano

    print("Iniciando processo de inserção de proposições...")

    # Se o ano for 0, processa todos os anos de 1950 até o ano atual
    if ano_especifico == 0:
        for ano in range(1950, ano_atual + 1):  # De 1950 até o ano atual (inclusive)
            print(f"\nProcessando proposições do ano {ano}...")
            try:
                inserir_proposicoes(ano)
                print(f"Concluído o processamento do ano {ano}.")
            except Exception as e:
                print(f"Erro ao processar o ano {ano}: {e}")
                break  # Para o loop se ocorrer algum erro
    else:
        # Se um ano específico for fornecido, processa apenas esse ano
        print(f"\nProcessando proposições do ano {ano_especifico}...")
        try:
            inserir_proposicoes(ano_especifico)
            print(f"Concluído o processamento do ano {ano_especifico}.")
        except Exception as e:
            print(f"Erro ao processar o ano {ano_especifico}: {e}")

    print("\nProcesso de inserção concluído.")
