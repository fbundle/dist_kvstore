import os
import sys
import uuid

from fire import Process, script_header

TMP_DIR = "tmp"


if __name__ == '__main__':
    os.makedirs(TMP_DIR, exist_ok=True)
    app1 = Process(
        task_name="app_100_69_15_9",
        host_name="khanh@100.69.15.9",
        deploy_dir="/tmp",
        tmux_path="/opt/homebrew/bin/tmux",
    )
    app2 = Process(
        task_name="app_100_93_62_117",
        host_name="khanh@100.93.62.117",
        deploy_dir="/tmp",
        tmux_path="/usr/bin/tmux",
    )

    # config
    import json

    open(f"{TMP_DIR}/config.json", "w").write(json.dumps([
        {
            "badger": "/Users/khanh/data/acceptor_100_69_15_9",
            "rpc": "100.69.15.9:4001",
            "store": "localhost:4000"
        },
        {
            "badger": "/home/khanh/data/acceptor_100_93_62_117",
            "rpc": "100.93.62.117:4001",
            "store": "localhost:4000"
        }
    ]))

    # clean
    clean = script_header
    clean += app1.clean().export()
    clean += app2.clean().export()
    open(f"{TMP_DIR}/clean", "w").write(clean)

    # run
    rpc_key =  str(uuid.uuid1())
    run = script_header
    run += app1.copy(src="bin/dist_kvstore").copy(src=f"{TMP_DIR}/config.json").exec(
        command="./dist_kvstore/run_darwin_arm64 config.json 0",
        env={"DIST_KVSTORE_RPC_KEY": rpc_key},
    ).export()
    run += app2.copy(src="bin/dist_kvstore").copy(src=f"{TMP_DIR}/config.json").exec(
        command="./dist_kvstore/run_linux_amd64 config.json 1",
        env={"DIST_KVSTORE_RPC_KEY": rpc_key},
    ).export()
    open(f"{TMP_DIR}/run", "w").write(run)

    if len(sys.argv) > 1:
        name = sys.argv[1]
        os.system(f"{TMP_DIR}/{name}")

