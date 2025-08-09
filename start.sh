#!/bin/bash

# Get the PORT from environment variable, default to 8000 if not set
PORT=${PORT:-8000}

# Start the uvicorn server
uvicorn test:app --host 0.0.0.0 --port $PORT