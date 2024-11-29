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

def inserir_autores_proposicoes(ano):
    url = f"https://dadosabertos.camara.leg.br/arquivos/proposicoesAutores/json/proposicoesAutores-{ano}.json"
    dados = obter_json_da_url(url)
    
    if not dados or "dados" not in dados:
        print("JSON inválido ou sem dados disponíveis.")
        return
    
    proposicoes_autores = dados["dados"]  # Lista de proposições com autores
    total_proposicoes = len(proposicoes_autores)
    print(f"{total_proposicoes} proposições com autores encontradas para o ano {ano}.")

    conexao = criar_conexao()
    if not conexao:
        print("Erro ao criar conexão com o banco de dados.")
        return
    
    try:
        
        apagar = conexao.cursor();
        apagar.execute(f'DELETE FROM proposicoes_autores WHERE proposicao_autor_ano = {ano}');
        
        cursor = conexao.cursor()

        # Barra de progresso para a inserção dos autores de proposições
        for index, proposicao_autor in enumerate(tqdm(proposicoes_autores, desc="Inserindo autores de proposições")):
            # Usando .get() para acessar as chaves com segurança
            proposicao_id = proposicao_autor.get("idProposicao", 0)  # Substituir por 0 se não existir
            autor_id = proposicao_autor.get("idDeputadoAutor", 0)  # Substituir por 0 se não existir
            autor_nome = proposicao_autor.get("nomeAutor", "")  # Substituir por string vazia se não existir
            autor_partido = proposicao_autor.get("siglaPartidoAutor", "")  # Substituir por string vazia se não existir
            autor_estado = proposicao_autor.get("siglaUFAutor", "")  # Substituir por string vazia se não existir
            proponente = proposicao_autor.get("proponente", 0)  # Substituir por 0 se não existir
            ordem_assinatura = proposicao_autor.get("ordemAssinatura", 0)  # Substituir por 0 se não existir
            
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
            sql = """
                INSERT INTO proposicoes_autores (
                    proposicao_id,
                    proposicao_autor_id,
                    proposicao_autor_nome,
                    proposicao_autor_partido,
                    proposicao_autor_estado,
                    proposicao_autor_proponente,
                    proposicao_autor_assinatura,
                    proposicao_autor_ano
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    proposicao_autor_nome = VALUES(proposicao_autor_nome),
                    proposicao_autor_partido = VALUES(proposicao_autor_partido),
                    proposicao_autor_estado = VALUES(proposicao_autor_estado),
                    proposicao_autor_proponente = VALUES(proposicao_autor_proponente),
                    proposicao_autor_assinatura = VALUES(proposicao_autor_assinatura),
                    proposicao_autor_ano = VALUES(proposicao_autor_ano)
            """
            valores = (
                proposicao_id,
                autor_id,
                autor_nome,
                autor_partido,
                autor_estado,
                proponente,
                ordem_assinatura,
                autor_ano,
            )

            cursor.execute(sql, valores)

        conexao.commit()
        print(f"Todas as {total_proposicoes} proposições com autores do ano {ano} foram inseridas com sucesso!")
    except Exception as e:
        print(f"Erro ao inserir proposições com autores: {e}")
        conexao.rollback()
    finally:
        fechar_conexao(conexao)
        print("Conexão com o banco de dados encerrada.")



if __name__ == "__main__":
    # Configuração do argparse para aceitar o argumento -ano
    parser = argparse.ArgumentParser(description="Processamento de proposições com autores.")
    parser.add_argument("-ano", type=int, default=0, help="Ano para inserir proposições (0 para todos os anos desde 1950).")
    args = parser.parse_args()

    ano_atual = datetime.now().year
    ano_especifico = args.ano

    print("Iniciando processo de inserção de proposições com autores...")

    # Se o ano for 0, processa todos os anos de 1950 até o ano atual
    if ano_especifico == 0:
        for ano in range(1950, ano_atual + 1):  # De 1950 até o ano atual (inclusive)
            print(f"\nProcessando proposições com autores do ano {ano}...")
            try:
                inserir_autores_proposicoes(ano)
                print(f"Concluído o processamento do ano {ano}.")
            except Exception as e:
                print(f"Erro ao processar o ano {ano}: {e}")
                break  # Para o loop se ocorrer algum erro
    else:
        # Se um ano específico for fornecido, processa apenas esse ano
        print(f"\nProcessando proposições com autores do ano {ano_especifico}...")
        try:
            inserir_autores_proposicoes(ano_especifico)
            print(f"Concluído o processamento do ano {ano_especifico}.")
        except Exception as e:
            print(f"Erro ao processar o ano {ano_especifico}: {e}")

    print("\nProcesso de inserção concluído.")
