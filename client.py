import json
import sys
from typing import Iterator

import requests

def make_request(method: str, addr: str, path: str, **kwargs) -> requests.Response:
    res = requests.request(method, f"{addr}/{path}", **kwargs)
    res.raise_for_status()
    return res

class KVStore:
    def __init__(self, addr: str = "http://localhost:4000"):
        self.addr = addr

    def __getitem__(self, key: str) -> str:
        return make_request("GET", self.addr, f"kvstore/{key}").text

    def next_token(self) -> int:
        return int(make_request("GET", self.addr, "kvstore_next").text)


    def set(self, token: int, key: str, value: str = ""):
        return make_request("PUT", self.addr, f"kvstore/{key}?token={token}", data=value)

    def keys(self) -> list[str]:
        return json.loads(make_request("GET", self.addr, "kvstore_keys").text)

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
