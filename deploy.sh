#!/bin/bash

sudo apt-get update && sudo apt-get upgrade -y
sudo apt-get install -y python3-pip python3-venv uvicorn

cd /home/ubuntu/TMI_api
git pull origin main

# virtual environemnt variable
VENV_DIR="venv"

# check if venv already exists
if [ -d "$VENV_DIR" ]; then
  echo "Virtual Environment already exists. Updating dependencies..."
  source $VENV_DIR/bin/activate
else
  echo "Virtual environment does not exist. Creating a new one..."
  python3 -m venv $VENV_DIR
  source $VENV_DIR/bin/activate
fi

# install dependencies
pip install --upgrade pip
pip install -r requirements.txt

# Check if fastapi is running on port 8000 and stop it
PID=$(lsof -t -i:8000)
if [ -n "$PID" ]; then
  echo "Stopping fastapi process with PID: $PID"
  kill -9 $PID
else
  echo "No fastapi process is running on port 8501"
fi

nohup uvicorn app:app --host 0.0.0.0 --port 8000 --workers 4 > fastapi.log 2>&1 &

echo "api deployment complete."
