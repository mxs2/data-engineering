import requests


class Extract:
    def __init__(self):
        pass

    def extract_country(self, country: str) -> list[dict]:
        """
        Método responsável por acessar a url e transformar o json em uma lista de dicionários.

        Args:
            country: str
        """
        url = f"http://universities.hipolabs.com/search?country={country}"
        response = requests.get(url)
        response.raise_for_status()
        universities = response.json()
        return universities
