import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import os

load_dotenv()

def criar_conexao():
    try:
        print("Iniciando conexão com o banco de dados...")
        db_port = int(os.getenv('DB_PORT'))
        
        if not all([os.getenv('DB_HOST'), os.getenv('DB_NAME'), os.getenv('DB_USER'), os.getenv('DB_PASS'), db_port]):
            raise ValueError("Faltam variáveis de ambiente obrigatórias no arquivo .env")

        conexao = mysql.connector.connect(
            host=os.getenv('DB_HOST'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASS'),
            database=os.getenv('DB_NAME'),
            port=db_port,
            auth_plugin='mysql_native_password'
        )
        
        if conexao.is_connected():
            #print("Conexão bem-sucedida ao MySQL!\n")
            return conexao
        
    except Error as e:
        print(f"Erro ao conectar ao MySQL: {e}")
        return None
    except ValueError as e:
        print(f"Erro: {e}")
        return None

def fechar_conexao(conexao):
    if conexao and conexao.is_connected():
        conexao.close()
        #print("Conexão encerrada.\n")