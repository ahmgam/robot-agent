#!/bin/bash
clear && ./clean_data.sh && docker image rm roschain:latest -f && docker-compose up -d
# Wait 15 seconds
sleep 15
python log.py
docker-compose down
