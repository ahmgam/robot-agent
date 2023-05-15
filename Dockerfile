FROM ros:noetic-ros-core-focal

RUN apt-get update 

RUN apt install -y git

RUN apt-get update && apt-get install --no-install-recommends -y \
    build-essential \
    python3-rosdep \
    python3-rosinstall \
    python3-vcstools \
    && rm -rf /var/lib/apt/lists/* 

RUN rosdep init && \
    rosdep update --rosdistro $ROS_DISTRO

RUN apt-get update && apt-get install -y --no-install-recommends \
    ros-noetic-ros-base=1.5.0-1* \
    ros-noetic-gazebo-ros-pkgs ros-noetic-gazebo-ros-control ros-noetic-interactive-markers ros-noetic-xacro \
    ros-noetic-dynamixel-sdk \
    libgflags-dev \
    libgoogle-glog-dev \
    protobuf-compiler libprotobuf-dev \
    apt-get install ros-noetic-map-server \
    && rm -rf /var/lib/apt/lists/*

RUN pip install pygame scipy

ENV TURTLEBOT3_MODEL=waffle               

RUN mkdir -p robot_ws/src
  
RUN git clone -b noetic-devel https://github.com/ROBOTIS-GIT/turtlebot3.git robot_ws/src/turtlebot3 

RUN git clone -b noetic-devel  https://github.com/ROBOTIS-GIT/turtlebot3_msgs.git robot_ws/src/turtlebot3_msgs

RUN git clone -b noetic-devel https://github.com/ROBOTIS-GIT/turtlebot3_simulations.git robot_ws/src/turtlebot3_simulations

RUN git clone  https://github.com/ethz-asl/rotors_simulator.git robot_ws/src/rotor_simulations

RUN git clone  https://github.com/OctoMap/octomap_msgs.git robot_ws/src/octomap_msgs

RUN git clone  https://github.com/ethz-asl/mav_comm.git robot_ws/src/mav_comm

RUN git clone  https://github.com/OctoMap/octomap_ros.git robot_ws/src/octomap_ros

RUN git clone https://github.com/ahmgam/multirobot_sim.git robot_ws/src/multirobot_sim

RUN /bin/bash -c '. /opt/ros/noetic/setup.bash; cd robot_ws; catkin_make'

COPY ./entrypoint.sh /

RUN chmod +x entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]

#CMD ["bash"]
