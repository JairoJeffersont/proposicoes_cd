import mysql.connector
from mysql.connector import Error

def criar_conexao():
    """Cria e retorna uma conexão com o banco de dados."""
    try:
        print("Aguardando conexão com o banco de dados...")
        conexao = mysql.connector.connect(
            host="localhost",          # Endereço do servidor
            user="root",        # Nome do usuário do MySQL
            password="root",      # Senha do usuário
            database="gabinete_digital",   # Nome do banco de dados
            port=8889
        )
        if conexao.is_connected():
            print("Conexão bem-sucedida!")
            return conexao
    except Error as e:
        print(f"Erro ao conectar ao MySQL: {e}")
        return None

def fechar_conexao(conexao):
    """Fecha a conexão com o banco de dados."""
    if conexao and conexao.is_connected():
        conexao.close()
        print("Conexão encerrada.")
