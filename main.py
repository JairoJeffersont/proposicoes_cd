from proposicoes.buscarProposicoes import inserirProposicoes, inserirAutoresProposicoes
import os
import shutil
from datetime import datetime


largura_terminal = shutil.get_terminal_size().columns

if __name__ == "__main__":
    os.system('clear')
    print("Iniciando busca de proposições e seus autores da Câmara dos Deputados.\n")
    
    while True:
        print('-' * largura_terminal)
        opcao = input("\n1 - Buscar proposições de um determinado ano\n2 - Buscar todas as proposições\n3 - Sair\n\nDigite a opção desejada: ")
        
        if opcao == '1':
            os.system('clear')
            ano = input("\nDigite o ano desejado: ")
            print(f'\nBuscando as proposições do ano {ano}. Isso pode levar alguns minutos...\n')
            inserirProposicoes(ano)
            inserirAutoresProposicoes(ano)

        elif opcao == '2':
            print('\nBuscando todas as proposições. Isso pode levar alguns minutos...\n')
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
                
        elif opcao == '3':
            print("Saindo...\n\n")
            break
        else:
            print("Opção inválida. Tente novamente.")

            
                
