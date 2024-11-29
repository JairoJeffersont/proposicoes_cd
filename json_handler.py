import requests

def obter_json_da_url(url):
    print(f"Acessando a url: {url}. Aguarde...")
    try:
        resposta = requests.get(url)
        resposta.raise_for_status()
        return resposta.json()
    except requests.exceptions.RequestException as e:
        print(f"Erro ao acessar a URL: {e}")
        return None
    except ValueError as e:
        print(f"Erro ao processar o JSON: {e}")
        return None
