import sys
import json
import os
import shutil

if __name__ == "__main__":
    app_name: str = sys.argv[1]
    host_list: list[str] = sys.argv[2].split(",")
    private_key: str = sys.argv[3]

    assert app_name == "dist_kvstore"

    config = {
        "private_key": private_key,
        "host_list": []
    }

    for host in host_list:
        config["host_list"].append({
            "badger": f"data/acceptor_{host}",
            "rpc": f"{host}:13800",
            "store": "localhost:4000",
        })
    
    if os.path.exists("tmp"):
        shutil.rmtree("tmp")
    os.makedirs("tmp")

    with open("tmp/config.json", "w") as f:
        f.write(json.dumps(config, indent=2))