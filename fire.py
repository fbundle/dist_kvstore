
from __future__ import annotations

import sys

copy_template = "rsync -avh --delete --progress {src} {host_name}:{deploy_dir}/{task_name}/"
exec_template = """ssh {host_name} << EOF
    set -xe
    {tmux_path} has-session -t {task_name} 2> /dev/null && {tmux_path} kill-session -t {task_name}
    cd {deploy_dir}/{task_name}
    {tmux_path} new-session -s {task_name} -d "export {env_str}; {command} |& tee {deploy_dir}/{task_name}/run.log"
EOF
"""
clean_template = """ssh {host_name} << EOF
    set -xe
    {tmux_path} has-session -t {task_name} 2> /dev/null && {tmux_path} kill-session -t {task_name}
    rm -rf {deploy_dir}/{task_name}
EOF
"""

warning_printed = False

def print_python_version_warning():
    global warning_printed
    if warning_printed:
        return
    warning_printed = True

    def get_python_version():
        return f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"

    tested_python_version = sorted({"3.12.", "3.13."})
    tested = False
    for python_version in tested_python_version:
        if get_python_version().startswith(python_version):
            tested = True
            break

    if not tested:
        print(f"WARNING: This script is only tested with python {tested_python_version}")

class Process:
    def __init__(self, task_name: str, host_name: str, deploy_dir: str = "/tmp", tmux_path: str = "tmux", commands: list[str] | None = None):
        print_python_version_warning()

        if commands is None:
            commands = []
        self.task_name = task_name
        self.host_name = host_name
        self.deploy_dir = deploy_dir
        self.tmux_path = tmux_path
        self.commands = commands

    def _append_command(self, command: str) -> Process:
        return Process(
            task_name=self.task_name,
            host_name=self.host_name,
            deploy_dir=self.deploy_dir,
            tmux_path=self.tmux_path,
            commands=self.commands + [command],
        )

    def copy(self, src: str) -> Process:
        return self._append_command(copy_template.format(
            src=src,
            host_name=self.host_name,
            deploy_dir=self.deploy_dir,
            task_name=self.task_name,
        ))

    def exec(self, command: str, env: dict[str, str] | None = None) -> Process:
        if env is None:
            env = {}
        env_str = " ".join([f"{k}={v}" for k, v in env.items()])
        return self._append_command(exec_template.format(
            host_name=self.host_name,
            deploy_dir=self.deploy_dir,
            task_name=self.task_name,
            tmux_path=self.tmux_path,
            env_str=env_str,
            command=command,
        ))

    def clean(self) -> Process:
        return self._append_command(clean_template.format(
            host_name=self.host_name,
            deploy_dir=self.deploy_dir,
            task_name=self.task_name,
            tmux_path=self.tmux_path,
        ))

    def export(self) -> str:
        return "\n".join(self.commands) + "\n"

script_header = """#!/usr/bin/env bash
set -xe
"""

__version__ = "0.0.1"