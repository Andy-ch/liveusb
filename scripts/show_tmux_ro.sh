#!/bin/bash
export DISPLAY=:0
nohup /usr/bin/qterminal -e 'tmux attach -r' &
