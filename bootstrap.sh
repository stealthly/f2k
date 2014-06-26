#!/bin/sh

JAVA_PATH=$(type -p java)
if [ -z "$JAVA_PATH" ]; then
    sudo apt-get -y update
    sudo apt-get install -y software-properties-common python-software-properties
    sudo add-apt-repository -y ppa:webupd8team/java
    sudo apt-get -y update
    sudo /bin/echo debconf shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections
    sudo apt-get -y install oracle-java7-installer oracle-java7-set-default
fi

export START_KAFKA_SCRIPT=https://raw2.github.com/stealthly/docker-kafka/master/start-broker.sh
curl -Ls $START_KAFKA_SCRIPT | bash /dev/stdin 1 9092 localhost