import os.path
import shutil
import subprocess
import sys

GO_COMPILER_MAP: dict[tuple[str, str], str] = {
    ("Darwin", "arm64"): "go1.24.4.darwin-arm64.tar.gz"
}

DEPLOYMENT_DIR: str = "/tmp/deploy"

def get_arch(host: str) -> tuple[str, str]:
    cmd = f"ssh {host} \"uname -s && uname -m\""
    result = subprocess.check_output(cmd, shell=True, text=True).splitlines()
    os_name = result[0].strip()
    machine = result[1].strip()
    return os_name, machine

def make_run_script(app: str, host: str, private_key: str) -> str:
    go_compiler = GO_COMPILER_MAP[get_arch(host)]
    go_compiler_path = f"go_compiler/{go_compiler}"
    deployment_dir = f"{DEPLOYMENT_DIR}/deploy_{app}_{host}"
    script = [
        f"rsync -avh --delete --progress {go_compiler_path} {host}:{deployment_dir}/",
        f"rsync -avh --delete --progress {app} {host}:{deployment_dir}/",
        f"""
ssh {host} << EOF
    cd {deployment_dir}
    rm -rf go
    tar -xf {go_compiler}
    export GOROOT={deployment_dir}/go
    export PATH=$GOROOT/bin:$PATH
    cd {app}
    go build -o main
    export PRIVATE_KEY={private_key}
    ./main |& tee {deployment_dir}/run.log
EOF
        """
    ]

    return "".join(map(lambda s: s + "\n", script))

def make_stop_script(app: str, host: str) -> str:
    deployment_dir = f"{DEPLOYMENT_DIR}/deploy_{app}_{host}"
    script = [
        f"ssh {host} \"rm -rf {deployment_dir}\""
    ]
    return "".join(map(lambda s: s + "\n", script))

if __name__ == "__main__":
    app, host_list_str, private_key = sys.argv[1], sys.argv[2], sys.argv[3]

    if os.path.exists("tmp"):
        shutil.rmtree("tmp")
    os.makedirs("tmp")
    with open(f"tmp/run_{app}", "w") as f:
        f.write("#!/usr/bin/env bash\n")
        f.write("set -e\n")
        for host in host_list_str.split(","):
            f.write(make_run_script(app, host, private_key))

    with open(f"tmp/stop_{app}", "w") as f:
        f.write("#!/usr/bin/env bash\n")
        f.write("set -e\n")
        for host in host_list_str.split(","):
            f.write(make_stop_script(app, host))



