Cassowary
=========

A Hive storage handler for Cassandra and Shark that reads the SSTables directly.
This allows total control over the resources used to run ad-hoc queries so that
the impact on real-time Cassandra performance is controlled.

Slides for a talk at Cassandra Europe on this project are here:

http://www.slideshare.net/RichardLow2/cassandra-europe-2013

and the video for the talk is here:

http://www.youtube.com/watch?v=QTb4HTwVMq0&list=PLqcm6qE9lgKLoYaakl3YwIWP4hmGsHm5e&index=2

Build
=====

To build this, you need the CQL storage handler from

https://github.com/milliondreams/hive/

on branch cas-support-cql. Clone this and build:

    $ git clone -b cas-support-cql git@github.com:milliondreams/hive.git
    $ cd hive/cassandra-handler
    $ mvn install

Then you can build cassowary:

    $ mvn package

Installation
============

You need Cassandra 1.2.x, Spark 0.8.x and Shark 0.8.x. To install Spark and Shark, follow

https://github.com/amplab/shark/wiki/Running-Shark-on-a-Cluster

Usage
=====

Launch your Shark shell with shark/bin/shark. Then add the cassowary jar to the classloader:

    shark> add jar /path/to/cassowary/target/cassowary-0.1-SNAPSHOT.jar;

Now you can create an external table referencing your Cassandra data e.g.:

    CREATE EXTERNAL TABLE users(username string, email string, `location` string, last_visited bigint)
    STORED BY 'com.wentnet.cassowary.storagehandler.SSTableStorageHandler'
    WITH SERDEPROPERTIES ("cassandra.host" = "192.168.0.25", "cassandra.sstable.rate_mb_s" = "10", "cassandra.yaml.location" = "/opt/cassandra/conf/cassandra.yaml")
    TBLPROPERTIES ("cassandra.cf.name" = "users", "cassandra.ks.name" = "ks");

and run queries:

    shark> select count(*) from users;
