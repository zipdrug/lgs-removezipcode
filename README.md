# lgs-removezipcode

For DEV,STG run
1. Change the docker variable RUN_ENV to development or STG
2. Build docker image by running command **./dbuild-dev.sh && ./drun-dev.sh**
3. If needed, To Create new Topic, Queue and subscribe topic run command **python topic_setup.py**
4. To push leads records to output topic run aommand **python main.py**
