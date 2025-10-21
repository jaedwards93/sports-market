from typing import Optional
import dotenv as e
import os
import requests
from .custom_errors import ConfigError

e.load_dotenv()


class ApiCall:
    def __init__(self, key_name: str, url: str, additional_params: Optional[dict] = None):
        if not os.getenv(key_name):
            raise ConfigError(f"No key exists in .env for key={key_name}")
        self.api_key = os.getenv(key_name)
        self.url = url
        self.params = {
            'api_key': self.api_key
        }
        if additional_params:
            self.params.update(additional_params)

    def run(self):
        data = requests.get(url=self.url, params=self.params)
        return data.json() if data.headers.get("Content-Type", "").startswith("application/json") else data
