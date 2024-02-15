#!/bin/bash
docker build -t roschain:latest .

docker run -d -v ${pwd}/data:/robot_ws/src/multirobot_sim/files \
--name sim-$ROBOT_NAME \
-e ROBOT_NAME=$ROBOT_NAME \
-e ROBOT_TYPE=$ROBOT_TYPE \
-e MQTT_HOST=$MQTT_HOST \
-e MQTT_PORT=$MQTT_PORT \
-e MQTT_USER=$MQTT_USER \
-e MQTT_PASS=$MQTT_PASS \
-e SECRET=$SECRET \
-e UPDATE_INTERVAL=UPDATE_INTERVAL \
roschain:latest