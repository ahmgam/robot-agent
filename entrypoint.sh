#!/bin/bash
set -e

# setup ros environment
source "/opt/ros/noetic/setup.bash" --
source "/robot_ws/devel/setup.bash" --
#roslaunch rotors_gazebo uav_launch.launch
roslaunch --wait --local -v /robot_ws/src/multirobot_sim/launch/blockchain.launch

exec "$@"