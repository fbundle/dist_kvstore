import json
import time
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
        return  make_request("GET", self.addr, f"kvstore/{key}").text

    def get(self, key: str, default: str = "") -> str:
        try:
            return self.__getitem__(key)
        except requests.exceptions.HTTPError as err:
            if err.response.status_code == 404: # not found
                return default
            raise err

    def __setitem__(self, key: str, value: str):
        make_request("PUT", self.addr, f"kvstore/{key}", data=value)

    def __delitem__(self, key: str):
        self.__setitem__(key, "")

    def keys(self) -> list[str]:
        return json.loads(make_request("GET", self.addr, "kvstore/").text)

    def __len__(self) -> int:
        return len(self.keys())

    def __contains__(self, key: str) -> bool:
        return key in self.keys()

    def __iter__(self) -> Iterator[str]:
        return iter(self.keys())

    def values(self) -> Iterator[str]:
        for k in self.keys():
            yield self.__getitem__(k)

    def items(self) -> Iterator[tuple[str, str]]:
        for k in self.keys():
            yield k, self.__getitem__(k)

    def __dict__(self) -> dict[str, str]:
        return dict(self.items())

    def __str__(self) -> str:
        return str(self.__dict__())

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(keys:[{', '.join(map(lambda s:f'"{s}"', self.keys()))}, ...])"

