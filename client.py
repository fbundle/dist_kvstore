import json

import requests

class KVStore:
    def __init__(self, addr: str = "http://localhost:4000/kvstore"):
        self.addr = addr
        self.keys = []

    def __getitem__(self, key: str) -> str:
        return requests.get(f"{self.addr}/{key}").text

    def __setitem__(self, key: str, value: str = ""):
        return requests.post(f"{self.addr}/{key}", data=value).text

    def __delitem__(self, key: str):
        return self.__setitem__(key, "")


    def keys(self) -> list[str]:
        text = requests.get(f"{self.addr}/").text
        return json.loads(text)
