#!/bin/sh
docker run -d -p 9200:9200 elasticsearch:5.5.2-alpine
until curl http://localhost:9200
do 
    sleep 5
done