import json

import requests

class KVStore:
    def __init__(self, addr: str):
        self.addr = addr

    def get(self, key: str) -> str:
        return requests.get(f"{self.addr}/{key}").text

    def set(self, key: str, value: str = ""):
        return requests.post(f'{self.addr}/{key}', data=value).text

    def keys(self) -> list[str]:
        text = requests.get(f"{self.addr}/").text
        return json.loads(text)
