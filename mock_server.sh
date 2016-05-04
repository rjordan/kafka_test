#!/bin/bash
docker rm kafka
docker run -d --name kafka -p 2181:2181 -p 9092:9092 --env ADVERTISED_HOST=192.168.0.60 --env ADVERTISED_PORT=9092 spotify/kafka
