#!/bin/bash

# Set the path to the robots directory
ROBOTS_DIR="/home/ubuntu/workspace/robot-agent/robots"

# Loop through all folders in the robots directory
for folder in "$ROBOTS_DIR"/*; do
    # Check if the folder is not the schema folder
    if [[ "$folder" != "$ROBOTS_DIR/schema" ]]; then
        # Delete the folder
        sudo rm -rf "$folder"
    fi
done
