#!/bin/bash

# Read the number of services from input
read -p "Enter the number of services: " n
read -p "Enter the number of cores per service: " cores
read -p "Enter the min number of connected robots to start: " min_nodes_num
# Docker Compose file name
compose_file="docker-compose.yml"
echo "version: '3.7'" >> $compose_file
echo "services:" >> $compose_file
# Loop to create services
for (( i=1; i<=n; i++  ))
do
  # Create service name by replacing  1 with the index
  service_name="robot$(printf "%02d" $i)"

  # Generate service definition
  service_definition=$(cat << EOF
  $service_name:
    image: roschain:latest
    build:
      context: .
      dockerfile: Dockerfile
    container_name: $service_name
    networks:
      - $service_name
    environment:
      - ROBOT_NAME=$service_name
      - ROBOT_TYPE=robot
      - MQTT_HOST=mosquittoo
      - MQTT_PORT=1883
      - SECRET=lkfpoewrvcmlsdjfjehf
      - UPDATE_INTERVAL=5
      - MIN_NODES_NUM=$min_nodes_num
    deploy:
      resources:
        limits:
          cpus: '$cores'
      restart_policy:
        condition: none
      placement:
        constraints:
          - node.role == worker
EOF
)

  # Append service definition to Docker Compose file
  echo "$service_definition" >> $compose_file
done

# Add mosquittoo service to Docker Compose file
mosquittoo_service=$(cat << EOF
  mosquittoo:
    image: eclipse-mosquitto
    container_name: mosquittoo
    restart: unless-stopped
    ports:
      - "1883:1883"
    volumes:
      - ./mosquitto/data:/etc/mosquitto
      - ./mosquitto/config:/mosquitto/config
    deploy:
      placement:
        constraints:
          - node.role == manager
    networks:\n
EOF
)

# Add networks for mosquittoo service
for (( i=1; i<=n; i++  ))
do
  service_name="robot$(printf "%02d" $i)"
  mosquittoo_service+="      - $service_name"$'\n'
done


# Append mosquittoo service to Docker Compose file
echo -e "$mosquittoo_service" >> $compose_file

echo "networks:" >> $compose_file

# Add networks section
for (( i=1; i<=n; i++  ))
do
  service_name="robot$(printf "%02d" $i)"
  echo "  $service_name:" >> $compose_file
done