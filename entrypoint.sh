#!/bin/bash
set -e

# setup ros environment
source "/opt/ros/noetic/setup.bash" --
source "/robot_ws/devel/setup.bash" --
#roslaunch rotors_gazebo uav_launch.launch

if [ -n "$ROBOT_NAME" ]
then
	echo "ROBOT_NAME is set"
else
  echo -e "ROBOT_NAME not set\n"
  exit
fi

if [ -n "$X" ]
then
	echo "X is set"
else
  echo -e "X not set\n"
  exit
fi

if [ -n "$Y" ]
then
	echo "Y is set"
else
  echo -e "Y not set\n"
  exit
fi

if [ -n "$ROBOT_TYPE" ]
then
  if [ "$ROBOT_TYPE" == "uav" ]
  then
    echo "robot type is uav"
    exit
  elif [ "$ROBOT_TYPE" == "ugv" ]
  then
    echo "robot type is ugv"
    exit
  else
    echo "robot type is shit"
    exit
  fi
else
  echo -e "ROBOT_TYPE not set\n"
fi
exec "$@"
