from typing import Iterator
import sys
import shutil
import os
import json

TMP_DIR = "tmp"
CONFIG_PATH = f"{TMP_DIR}/config.json"
RUN_PATH = f"{TMP_DIR}/run_all.sh"
STOP_PATH = f"{TMP_DIR}/stop_all.sh"
GOBIN = "golang/go/bin/go"
TMUX_SESSION = "kvstore"
AES_KEY = "AES_KEY"

if __name__ == "__main__":
    # aes_key = "this is an aes key"
    # addr_list_str = "192.168.1.100,192.168.1.101,192.168.1.102"
    aes_key = sys.argv[1]
    addr_list_str = sys.argv[2]

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
            command = f"rsync -avh --progress --exclude \"data\" {cwd} {addr}:{target}/ &"
            yield command
        
        yield "wait"

        # run node script
        for i, addr in enumerate(addr_list):
            node_command = ""
            node_command += f"tmux has-session -t {TMUX_SESSION} 2>/dev/null && tmux kill-session -t {TMUX_SESSION}"
            node_command += "; "
            node_command += f"tmux new-session -s {TMUX_SESSION} -d \\\"cd {cwd}; {AES_KEY}=\"{aes_key}\" {GOBIN} run main.go {CONFIG_PATH} {i} |& tee run.log\\\""
            command = f"ssh {addr} \'bash -lc \"{node_command}\"\'"
            yield command
    
    def make_stop_command_list() -> Iterator[str]:
        for i, addr in enumerate(addr_list):
            node_command = ""
            node_command += f"tmux has-session -t {TMUX_SESSION} 2>/dev/null && tmux kill-session -t {TMUX_SESSION}"
            command = f"ssh {addr} \'bash -lc \"{node_command}\"\'"
            yield command


    if os.path.exists(TMP_DIR):
        shutil.rmtree(TMP_DIR)
    
    os.makedirs(TMP_DIR)
    # copy go binary
    shutil.copy(GOBIN, TMP_DIR)

    # write config
    with open(CONFIG_PATH, "w") as f:
        f.write(json.dumps(list(make_config_list()), indent=4))
    
    # generate command
    with open(RUN_PATH, "w") as f:
        for line in make_command_list():
            f.write(line + "\n")
    
    with open(STOP_PATH, "w") as f:
        for line in make_stop_command_list():
            f.write(line + "\n")
    
