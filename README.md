File Loader to Kafka
====================

Enables you to mirror(download and upload) directory structure with all files or a single file uploaded to kafka from topic.

Instructions to assemble
=========================

    ./gradlew clean fatJar        
Once it's done, jar file is going to be available in build/libs/ directory

Instructions to setup environment
=================================

To launch Docker development enviornment

    ./bootstrap.sh 

Once it's done you have kafka and zookeeper up and running at localhost on ports 9092 and 2181 respectively 

Instructions to run in environment
==================================

To upload 

    java -jar build/libs/f2k-1.0.jar upload /path/to/dir/or/file/to/upload topicName localhost:9092 (false|true) (protobuf|avro)

To download 

    java -jar build/libs/f2k-1.0.jar download /path/to/dir/or/file/to/download topicName localhost:2181 (protobuf|avro)
