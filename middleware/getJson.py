import requests

def getJson(url):
    print(f"Acessando a url: {url}. Aguarde...\n")
    try:
        resposta = requests.get(url)
        resposta.raise_for_status()
        return resposta.json()
    except requests.exceptions.RequestException as e:
        print(f"Erro ao acessar a URL\n")
        return None

