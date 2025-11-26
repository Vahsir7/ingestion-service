#!/bin/bash

SESSION="log_ingestor"

# Kill previous session if exists
tmux kill-session -t $SESSION 2>/dev/null

# Create new tmux session
tmux new-session -d -s $SESSION

# Terminal 1: Docker Compose
tmux rename-window -t $SESSION:0 'docker'
tmux send-keys -t $SESSION:0 "
echo 'Starting Docker Compose...';
docker-compose up -d
" C-m

# Terminal 2: Go ingestion service
tmux new-window -t $SESSION:1 -n 'ingestion'
tmux send-keys -t $SESSION:1 "
cd ingestion-service;
echo 'Starting Go ingestion service...';
go run main.go
" C-m

# Teminal 3: Python processor
tmux new-window -t $SESSION:2 -n 'processor'
tmux send-keys -t $SESSION:2 "
cd processor-service;

if [ ! -d '.venv' ]; then
    python3 -m venv .venv;
fi

source .venv/bin/activate;
pip install -r requirements.txt;
python main.py
" C-m

echo 'All services launched inside tmux session: log_ingestor'
echo 'Attach using: tmux attach -t log_ingestor'
echo 'To switch between panes: Ctrl+b then number (0,1,2)'
echo 'Detach using: Ctrl+b then d'


