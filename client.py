import json

import requests

class KVStore:
    def __init__(self, addr: str = "http://localhost:4000/kvstore"):
        self.addr = addr

    def __getitem__(self, key: str) -> str:
        res = requests.get(f"{self.addr}/{key}")
        if res.status_code != 200:
            print(f"Error: {res.text}")
            return ""
        return res.text

    def __setitem__(self, key: str, value: str = ""):
        res = requests.post(f"{self.addr}/{key}", data=value)
        if res.status_code != 200:
            print(f"Error: {res.text}")
            return ""
        return res.text

    def __delitem__(self, key: str):
        return self.__setitem__(key, "")


    def keys(self) -> list[str]:
        res = requests.get(f"{self.addr}/")
        if res.status_code != 200:
            print(f"Error: {res.text}")
            return []
        return json.loads(res.text)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(keys:{self.keys()})"

    def __dict__(self) -> dict[str, str]:
        return {k: self.__getitem__(k) for k in self.keys()}
