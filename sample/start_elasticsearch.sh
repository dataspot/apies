#!/bin/sh
docker run -d -p 9200:9200 -e discovery.type=single-node -e xpack.security.enabled=false elasticsearch:7.4.2
# docker run -d -p 9200:9200 -e xpack.security.enabled=false elasticsearch:5
until curl http://localhost:9200
do 
    sleep 5
done
