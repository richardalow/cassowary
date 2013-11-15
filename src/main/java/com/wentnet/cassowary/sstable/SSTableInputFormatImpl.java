package com.wentnet.cassowary.sstable;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.hadoop.ColumnFamilyInputFormat;
import org.apache.cassandra.hadoop.ColumnFamilySplit;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.cassandra.serde.AbstractCassandraSerDe;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.wentnet.cassowary.conf.SSTableConfiguration;

public class SSTableInputFormatImpl implements InputFormat<MapWritable, MapWritable> {

	private static final Logger logger = LoggerFactory.getLogger(SSTableInputFormatImpl.class);

	protected static void setCassandraYamlLocation(String path) throws IOException {
		URI uri;
		try {
			uri = new URI(path);
		} catch (URISyntaxException e) {
			throw new IOException("Cannot parse path '" + path + "'", e);
		}
		if (uri.getScheme() == null)
			path = "file://" + path;
		System.setProperty("cassandra.config", path);
	}

	@Override
	public RecordReader<MapWritable, MapWritable> getRecordReader(InputSplit split, JobConf jobConf, final Reporter reporter) throws IOException {
		setCassandraYamlLocation(jobConf.get(SSTableConfiguration.CASSANDRA_YAML_LOCATION));

		SSTableSplit sstableSplit = (SSTableSplit) split;

		try {
			return new SSTableRecordReader(sstableSplit, jobConf);
		} catch (ConfigurationException e) {
			throw new IOException(e);
		}
	}

	@Override
	public InputSplit[] getSplits(JobConf jobConf, int numSplits) throws IOException {
		final String ks = jobConf.get(AbstractCassandraSerDe.CASSANDRA_KEYSPACE_NAME);
		final String cf = jobConf.get(AbstractCassandraSerDe.CASSANDRA_CF_NAME);
		final int splitSize = jobConf.getInt(AbstractCassandraSerDe.CASSANDRA_SPLIT_SIZE, AbstractCassandraSerDe.DEFAULT_SPLIT_SIZE);
		final int rpcPort = jobConf.getInt(AbstractCassandraSerDe.CASSANDRA_PORT, Integer.parseInt(AbstractCassandraSerDe.DEFAULT_CASSANDRA_PORT));
		final String host = jobConf.get(AbstractCassandraSerDe.CASSANDRA_HOST);
		final String partitionerString = jobConf.get(AbstractCassandraSerDe.CASSANDRA_PARTITIONER);
		final String cassandraColumnMapping = jobConf.get(AbstractCassandraSerDe.CASSANDRA_COL_MAPPING);

		if (cassandraColumnMapping == null) {
			throw new IOException("cassandra.columns.mapping required for Cassandra Table.");
		}

		final Path dummyPath = new Path(ks + "/" + cf);

		SliceRange range = new SliceRange();
		range.setStart(new byte[0]);
		range.setFinish(new byte[0]);
		range.setReversed(false);
		range.setCount(Integer.MAX_VALUE);
		SlicePredicate predicate = new SlicePredicate();
		predicate.setSlice_range(range);

		ConfigHelper.setInputPartitioner(jobConf, partitionerString);
		ConfigHelper.setInputColumnFamily(jobConf, ks, cf);
		ConfigHelper.setInputSplitSize(jobConf, splitSize);
		ConfigHelper.setInputInitialAddress(jobConf, host);
		ConfigHelper.setInputSlicePredicate(jobConf, predicate);
		ConfigHelper.setInputRpcPort(jobConf, Integer.toString(rpcPort));

		ColumnFamilyInputFormat cfif = new ColumnFamilyInputFormat();
		InputSplit[] cfifSplits = cfif.getSplits(jobConf, numSplits);
		InputSplit[] results = new InputSplit[cfifSplits.length];
		for (int i = 0; i < cfifSplits.length; i++) {
			ColumnFamilySplit cfSplit = (ColumnFamilySplit) cfifSplits[i];
			SSTableSplit split = new SSTableSplit(cassandraColumnMapping, cfSplit.getStartToken(), cfSplit.getEndToken(), cfSplit.getLocations(), dummyPath);
			split.setKeyspace(ks);
			split.setColumnFamily(cf);
			split.setEstimatedRows(cfSplit.getLength());
			split.setPartitioner(partitionerString);
			results[i] = split;
			logger.debug("Created split: {}", split);
		}

		return results;
	}
}
