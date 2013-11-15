package com.wentnet.cassowary.sstable;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.hadoop.hive.cassandra.serde.cql.CqlSerDe;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.RateLimiter;
import com.wentnet.cassowary.conf.SSTableConfiguration;

public class SSTableRecordReader implements RecordReader<MapWritable, MapWritable> {
	private static final Logger logger = LoggerFactory.getLogger(SSTableRecordReader.class);

	private final ColumnFamilyMetadata metadata;
	private final Text[] keyAliasNames;
	private final Text[] columnAliasNames;
	private final MapWritable keyWritable = new MapWritable();
	private final MapWritable valueWritable = new MapWritable();
	private final RowPosition startPosition;
	private final RowPosition endPosition;
	private final IPartitioner<Token<?>> partitioner;
	private final List<String> columnNames;
	private final RateLimiter rateLimiter;
	private final long estimatedRows;

	private long completedRows = 0l;
	private HiveRowIterator hiveRowIterator;

	protected static boolean containsLocalAddress(String... hostnames) throws UnknownHostException {
		final InetAddress localhost = InetAddress.getLocalHost();
		for (String hostname : hostnames) {
			final InetAddress[] addresses = InetAddress.getAllByName(hostname);
			for (InetAddress remoteAddress : addresses)
				if (remoteAddress.equals(localhost))
					return true;
		}
		return false;
	}

	private static Text[] convertAliasList(List<String> aliases) {
		Text[] aliasNames = new Text[aliases.size()];
		for (int i = 0; i < aliases.size(); i++)
			aliasNames[i] = new Text(aliases.get(i));
		return aliasNames;
	}

	@SuppressWarnings("unchecked")
	public SSTableRecordReader(SSTableSplit split, JobConf conf) throws IOException, ConfigurationException {
		if (!containsLocalAddress(split.getLocations()))
			throw new IOException("Cannot read remotely, trying to read split " + split);

		metadata = ColumnFamilyMetadata.fromConf(split.getKeyspace(), split.getColumnFamily(), conf);
		keyAliasNames = convertAliasList(metadata.getKeyAliases());
		columnAliasNames = convertAliasList(metadata.getColumnAliases());
		columnNames = CqlSerDe.parseColumnMapping(split.getColumnMapping());
		estimatedRows = split.getEstimatedRows();

		logger.info("Starting record reader for tokens [{}, {}), estimated rows " + estimatedRows, split.getStartToken(), split.getEndToken());

		partitioner = FBUtilities.newPartitioner(split.getPartitioner());
		final Token<?> startKey = partitioner.getTokenFactory().fromString(split.getStartToken());
		// TODO check these min/max are correct
		startPosition = startKey.minKeyBound();
		final Token<?> endKey = partitioner.getTokenFactory().fromString(split.getEndToken());
		endPosition = endKey.minKeyBound();

		final String rateLimitMBsString = conf.get(SSTableConfiguration.CASSANDRA_SSTABLE_RATE_MB_S);
		if (rateLimitMBsString == null)
			logger.info("Rate limiting disabled");
		else
			logger.info("Rate limiting to {} MB/s", rateLimitMBsString);

		rateLimiter = rateLimitMBsString == null ? null : RateLimiter.create(((double) Float.parseFloat(rateLimitMBsString)) * 1024.0d * 1024.0d);
	}

	@Override
	public boolean next(MapWritable key, MapWritable value) throws IOException {
		// if we're starting out
		if (hiveRowIterator == null)
			hiveRowIterator = new HiveRowIterator(startPosition, endPosition, partitioner, keyAliasNames, columnAliasNames, metadata, columnNames, rateLimiter);

		if (!hiveRowIterator.hasNext())
			return false;

		Pair<MapWritable, MapWritable> nextPair = hiveRowIterator.next();

		key.clear();
		key.putAll(nextPair.left);
		value.clear();
		value.putAll(nextPair.right);

		return true;
	}

	@Override
	public MapWritable createKey() {
		return keyWritable;
	}

	@Override
	public MapWritable createValue() {
		return valueWritable;
	}

	@Override
	public long getPos() throws IOException {
		return 0l;
	}

	@Override
	public void close() throws IOException {
		hiveRowIterator.close();
	}

	@Override
	public float getProgress() throws IOException {
		if (estimatedRows == 0l)
			return 1.0f;
		return Math.min((float) completedRows / (float) estimatedRows, 1.0f);
	}
}
