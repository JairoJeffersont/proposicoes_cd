from proposicoes.buscarProposicoes import inserirProposicoes, inserirAutoresProposicoes, criarSqlProposicoes,criarSqlAutores
import os
import shutil
from datetime import datetime


largura_terminal = shutil.get_terminal_size().columns

if __name__ == "__main__":
    os.system('clear')
    print("Iniciando busca de proposições e seus autores da Câmara dos Deputados.\n")
    
    while True:
        print('-' * largura_terminal)
        opcao = input("\n1 - Inserir proposições de um determinado ano\n2 - Inserir todas as proposições\n3 - Criar SQL de um ano especifico\n4 - Criar SQL com todas as proposicoes\n5 - Sair\n\nDigite a opção desejada: ")
        
        if opcao == '1':
            os.system('clear')
            ano = input("\nDigite o ano desejado: ")
            print(f'\nBuscando as proposições do ano {ano}. Isso pode levar alguns minutos...\n')
            inserirProposicoes(ano)
            inserirAutoresProposicoes(ano)

        elif opcao == '2':
            # Exibe a mensagem de aviso
            resposta = input('\nTem certeza que deseja continuar? Esse procedimento pode levar vários minutos. (s/n): ').strip().lower()

            # Verifica a resposta do usuário
            if resposta == 's':
                print('\nBuscando todas as proposições.\n')
                ano_atual = datetime.now().year
                for ano in range(1950, ano_atual + 1):
                    print(f"\nProcessando proposições do ano {ano}...")
                    try:
                        inserirProposicoes(ano)
                        inserirAutoresProposicoes(ano)
                        print(f"Concluído o processamento do ano {ano}.")
                    except Exception as e:
                        print(f"Erro ao processar o ano {ano}: {e}")
                        break  # Para o loop se ocorrer algum erro
            else:
                print("\nProcesso cancelado pelo usuário.")
                
        elif opcao == '3':
            ano = input("\nDigite o ano desejado: ")
            print(f'Criando SQL com as proposições do ano {ano}. Isso pode levar alguns minutos...\n')
            criarSqlProposicoes(ano)
            criarSqlAutores(ano)
            
        elif opcao == '4':
            resposta = input('\nTem certeza que deseja continuar? Esse procedimento pode levar vários minutos. (s/n): ').strip().lower()
            pasta_sql = 'sql'
            
            if resposta == 's':
                print('\nBuscando todas as proposições.\n')
                
                # Limpar a pasta sql antes de iniciar o processamento
                if os.path.exists(pasta_sql) and os.path.isdir(pasta_sql):
                    for arquivo in os.listdir(pasta_sql):
                        caminho_arquivo = os.path.join(pasta_sql, arquivo)
                        try:
                            if os.path.isfile(caminho_arquivo):
                                os.remove(caminho_arquivo)  # Apaga o arquivo
                        except Exception as e:
                            print(f'Erro ao tentar apagar {arquivo}: {e}')
                
                ano_atual = datetime.now().year
                for ano in range(1950, ano_atual + 1):
                    print(f"\nProcessando proposições do ano {ano}...")
                    try:
                        criarSqlProposicoes(ano)
                        criarSqlAutores(ano)
                        print(f"Concluído o processamento do ano {ano}.")
                    except Exception as e:
                        print(f"Erro ao processar o ano {ano}: {e}")
                        break  # Para o loop se ocorrer algum erro
            else:
                print("\nProcesso cancelado pelo usuário.")
        
        elif opcao == '5':
            print("Saindo...\n\n")
            break
        else:
            print("Opção inválida. Tente novamente.")

            
                
