import json
import sys
from typing import Iterator

import requests

type HTTPError = requests.Response

class KVStore:
    def __init__(self, addr: str = "http://localhost:4000"):
        self.addr = addr

    def __getitem__(self, key: str) -> str:
        res = requests.get(f"{self.addr}/kvstore/{key}")
        res.raise_for_status()
        return res.text

    def next_token(self) -> int:
        res = requests.get(f"{self.addr}/kvstore_next")
        res.raise_for_status()
        return int(res.text)


    def set(self, token: int, key: str, value: str = ""):
        res = requests.post(f"{self.addr}/kvstore/{key}?token={token}", data=value)
        res.raise_for_status()

    def __delitem__(self, token: int, key: str):
        return self.set(token, key, "")


    def keys(self) -> list[str]:
        res = requests.get(f"{self.addr}/kvstore_keys")
        res.raise_for_status()
        return json.loads(res.text)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(keys:{self.keys()})"

    def values(self) -> Iterator[str]:
        for k in self.keys():
            yield self.__getitem__(k)

    def items(self) -> Iterator[tuple[str, str]]:
        for k in self.keys():
            yield k, self.__getitem__(k)

    def __dict__(self) -> dict[str, str]:
        return dict(self.items())
