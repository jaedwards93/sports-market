import dotenv as e
import os
import requests


class ApiCall:
    def __init__(self, key_name, url, additional_params=None):
        e.load_dotenv()
        self.url = url
        self.api_key = os.getenv(key_name)
        self.params = {
            'api_key': self.api_key
        }
        if additional_params:
            self.params.update(additional_params)

    def run(self):
        data = requests.get(url=self.url, params=self.params)
        return data.json() if data.headers.get("Content-Type", "").startswith("application/json") else data
