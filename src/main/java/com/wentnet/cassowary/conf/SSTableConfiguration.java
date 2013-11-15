package com.wentnet.cassowary.conf;

public class SSTableConfiguration {

	public static final String CASSANDRA_SSTABLE_RATE_MB_S = "cassandra.sstable.rate_mb_s";
	public static final String CASSANDRA_YAML_LOCATION = "cassandra.yaml.location";
	public static final String DEFAULT_CASSANDRA_YAML_LOCATION = "/etc/cassandra/conf/cassandra.yaml";

	public static final String CASSANDRA_CQL_PORT = "cassandra.cql.port";

	public static final String DEFAULT_CASSANDRA_CQL_PORT = "9042";
	public static final String DEFAULT_PARTITIONER = "org.apache.cassandra.dht.Murmur3Partitioner";
}
