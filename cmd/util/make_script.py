from typing import Iterator
import sys
import os
import json

TMP_DIR = "tmp"
CONFIG_PATH = f"{TMP_DIR}/config.json"
RUN_PATH = f"{TMP_DIR}/run"
TMUX_SESSION = "kvstore"

if __name__ == "__main__":
    addr_list_str = sys.argv[1]
    addr_list = addr_list_str.split(",")
    cwd = os.getcwd()

    def make_config_list() -> Iterator:
        for addr in addr_list:
            yield {
                "badger": f"data/acceptor_{addr}",
                "rpc": f"{addr}:13800",
                "store": "localhost:4000",
            }

    def make_command_list() -> Iterator[str]:
        # copy code
        for addr in addr_list:
            target = os.path.dirname(cwd)
            command = f"rsync -avh --progress {cwd} {addr}:{target}/ &"
            yield command
        
        yield "wait"

        # run node script
        for i, addr in enumerate(addr_list):
            node_command = ""
            node_command += f"tmux has-session -t {TMUX_SESSION} 2>/dev/null && tmux kill-session -t {TMUX_SESSION}"
            node_command += "; "
            node_command += f"tmux new-session -s {TMUX_SESSION} -d \"cd {cwd}; go run main.go {CONFIG_PATH} {i} |& tee run.log\""
            command = f"ssh {addr} \'{node_command}\'"
            yield command


    if not os.path.exists(TMP_DIR):
        os.makedirs(TMP_DIR)
    # write config
    with open(CONFIG_PATH, "w") as f:
        f.write(json.dumps(list(make_config_list()), indent=4))
    
    # generate command
    with open(RUN_PATH, "w") as f:
        for line in make_command_list():
            f.write(line + "\n")
    
