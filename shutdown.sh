#!/bin/sh

docker kill zkserver
docker kill broker1

docker rm broker1
docker rm zkserver