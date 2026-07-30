#!/usr/bin/env zsh
set -euo pipefail

print -u2 "launch_thyphon.zsh is kept for compatibility. Use launch_distributed_runtime.zsh or launch_simulator.zsh explicitly."
exec zsh "${0:A:h}/launch_distributed_runtime.zsh"
