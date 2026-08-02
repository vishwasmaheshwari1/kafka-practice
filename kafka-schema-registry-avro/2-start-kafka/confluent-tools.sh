#!/bin/bash

# Two options to get access to the confluent tools

# Option 1:
# Download the confluent binaries at:
# https://www.confluent.io/download/
# Put them on your system and put the confluent/bin directory in your path
kafka-avro-console-consumer.sh

# Option 2:
# Use a docker image to have access to all the binaries right away:

# docker run -it --rm --net=host confluentinc/cp-schema-registry:3.3.1 bash
docker run -it --rm --net=host confluentinc/cp-schema-registry:8.2.2 bash
# Then you can do 
kafka-avro-console-consumer




# url to check on schema registry details
# http://localhost:8081 ->  schema registry is hosted
http://localhost:8081/subjects/customer-avro-value/versions
http://localhost:8081/subjects/customer-avro-value/versions/latest
http://localhost:8081/schemas/ids/1
