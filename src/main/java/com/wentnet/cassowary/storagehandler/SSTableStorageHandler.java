package com.wentnet.cassowary.storagehandler;

import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.cassandra.output.cql.HiveCqlOutputFormat;
import org.apache.hadoop.hive.cassandra.serde.AbstractCassandraSerDe;
import org.apache.hadoop.hive.cassandra.serde.cql.CqlSerDe;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.api.Constants;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.wentnet.cassowary.conf.SSTableConfiguration;
import com.wentnet.cassowary.sstable.ColumnFamilyMetadata;
import com.wentnet.cassowary.sstable.SSTableInputFormat;

public class SSTableStorageHandler implements HiveStorageHandler, HiveMetaHook, HiveStoragePredicateHandler {

	private static final Logger logger = LoggerFactory.getLogger(SSTableStorageHandler.class);

	private Configuration configuration;

	@Override
	public void configureTableJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
		Properties tableProperties = tableDesc.getProperties();

		// Identify Keyspace
		String keyspace = tableProperties.getProperty(AbstractCassandraSerDe.CASSANDRA_KEYSPACE_NAME);
		if (keyspace == null) {
			keyspace = tableProperties.getProperty(Constants.META_TABLE_DB);
		}
		jobProperties.put(AbstractCassandraSerDe.CASSANDRA_KEYSPACE_NAME, keyspace);

		// Identify ColumnFamily
		String columnFamily = tableProperties.getProperty(AbstractCassandraSerDe.CASSANDRA_CF_NAME);
		if (columnFamily == null) {
			columnFamily = tableProperties.getProperty(Constants.META_TABLE_NAME);
		}
		jobProperties.put(AbstractCassandraSerDe.CASSANDRA_CF_NAME, columnFamily);

		// If no column mapping has been configured, we should create the
		// default column mapping.
		String columnInfo = tableProperties.getProperty(AbstractCassandraSerDe.CASSANDRA_COL_MAPPING);
		if (columnInfo == null) {
			columnInfo = CqlSerDe.createColumnMappingString(tableProperties.getProperty(org.apache.hadoop.hive.serde.Constants.LIST_COLUMNS));
			logger.debug("Auto created column mapping {}", columnInfo);
		} else
			logger.debug("Got column mapping from properties {}", columnInfo);
		jobProperties.put(AbstractCassandraSerDe.CASSANDRA_COL_MAPPING, columnInfo);

		String host = configuration.get(AbstractCassandraSerDe.CASSANDRA_HOST);
		if (host == null) {
			host = tableProperties.getProperty(AbstractCassandraSerDe.CASSANDRA_HOST, AbstractCassandraSerDe.DEFAULT_CASSANDRA_HOST);
		}
		jobProperties.put(AbstractCassandraSerDe.CASSANDRA_HOST, host);

		String port = configuration.get(AbstractCassandraSerDe.CASSANDRA_PORT);
		if (port == null) {
			port = tableProperties.getProperty(AbstractCassandraSerDe.CASSANDRA_PORT, AbstractCassandraSerDe.DEFAULT_CASSANDRA_PORT);
		}
		jobProperties.put(AbstractCassandraSerDe.CASSANDRA_PORT, port);

		String cqlPort = configuration.get(SSTableConfiguration.CASSANDRA_CQL_PORT);
		if (cqlPort == null) {
			cqlPort = tableProperties.getProperty(SSTableConfiguration.CASSANDRA_CQL_PORT, SSTableConfiguration.DEFAULT_CASSANDRA_CQL_PORT);
		}
		jobProperties.put(SSTableConfiguration.CASSANDRA_CQL_PORT, cqlPort);

		if (configuration.get(AbstractCassandraSerDe.CASSANDRA_PARTITIONER) == null) {
			jobProperties.put(AbstractCassandraSerDe.CASSANDRA_PARTITIONER,
					tableProperties.getProperty(AbstractCassandraSerDe.CASSANDRA_PARTITIONER, SSTableConfiguration.DEFAULT_PARTITIONER));
		} else {
			jobProperties.put(AbstractCassandraSerDe.CASSANDRA_PARTITIONER, configuration.get(AbstractCassandraSerDe.CASSANDRA_PARTITIONER));
		}

		String rateLimitMBsString = tableProperties.getProperty(SSTableConfiguration.CASSANDRA_SSTABLE_RATE_MB_S, "0");
		try {
			Float.parseFloat(rateLimitMBsString);
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("Cannot parse '" + rateLimitMBsString + "' to float");
		}
		jobProperties.put(SSTableConfiguration.CASSANDRA_SSTABLE_RATE_MB_S, rateLimitMBsString);

		String cassandraYamlLocation = tableProperties.getProperty(SSTableConfiguration.CASSANDRA_YAML_LOCATION,
				SSTableConfiguration.DEFAULT_CASSANDRA_YAML_LOCATION);
		jobProperties.put(SSTableConfiguration.CASSANDRA_YAML_LOCATION, cassandraYamlLocation);

		final ColumnFamilyMetadata metadata = getColumnFamilyMetadata(keyspace, columnFamily, host, Integer.parseInt(cqlPort));
		logger.debug("Got column family metadata {}", metadata);
		metadata.writeToMap(jobProperties);
	}

	private static ColumnFamilyMetadata getColumnFamilyMetadata(String ks, String cf, String host, int rpcPort) {
		final Cluster cluster = Cluster.builder().addContactPoint(host).withPort(rpcPort).build();
		final Session session = cluster.connect();

		try {
			final PreparedStatement statement = session
					.prepare("SELECT key_aliases, column_aliases, key_validator, comparator from system.schema_columnfamilies "
							+ "WHERE keyspace_name = ? AND columnfamily_name = ?;");
			final BoundStatement boundStatement = new BoundStatement(statement);
			final ResultSet results = session.execute(boundStatement.bind(ks, cf));
			final Row row = results.one();
			if (row == null)
				return null;
			return new ColumnFamilyMetadata(ks, cf, row.getString("key_aliases"), row.getString("column_aliases"), row.getString("key_validator"),
					row.getString("comparator"));
		} finally {
			session.shutdown();
		}
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Class<? extends InputFormat> getInputFormatClass() {
		return SSTableInputFormat.class;
	}

	@Override
	public HiveMetaHook getMetaHook() {
		return this;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Class<? extends OutputFormat> getOutputFormatClass() {
		// Not sure what to do here, since we don't support writing
		return HiveCqlOutputFormat.class;
	}

	@Override
	public Class<? extends SerDe> getSerDeClass() {
		return CqlSerDe.class;
	}

	@Override
	public Configuration getConf() {
		return this.configuration;
	}

	@Override
	public void setConf(Configuration configuration) {
		this.configuration = configuration;
	}

	@Override
	public void preCreateTable(Table table) throws MetaException {
		logger.error("Can't create SSTable tables");
	}

	@Override
	public void commitCreateTable(Table table) throws MetaException {
		// No work needed
	}

	@Override
	public void commitDropTable(Table table, boolean deleteData) throws MetaException {

	}

	@Override
	public void preDropTable(Table table) throws MetaException {
		// nothing to do
	}

	@Override
	public void rollbackCreateTable(Table table) throws MetaException {
		// No work needed
	}

	@Override
	public void rollbackDropTable(Table table) throws MetaException {
		// nothing to do
	}

	@Override
	public HiveAuthorizationProvider getAuthorizationProvider() throws HiveException {
		return null;
	}

	@Override
	public void configureInputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
		configureTableJobProperties(tableDesc, jobProperties);
	}

	@Override
	public void configureOutputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
		configureTableJobProperties(tableDesc, jobProperties);
	}

	@Override
	public DecomposedPredicate decomposePredicate(JobConf jobConf, Deserializer deserializer, ExprNodeDesc predicate) {
		return null;
	}

}
