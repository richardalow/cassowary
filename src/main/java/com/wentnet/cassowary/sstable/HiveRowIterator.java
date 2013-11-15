package com.wentnet.cassowary.sstable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.cassandra.cql3.CFDefinition;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.Pair;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.RateLimiter;

public class HiveRowIterator implements CloseableIterator<Pair<MapWritable, MapWritable>> {

	private static final Logger logger = LoggerFactory.getLogger(HiveRowIterator.class);

	private final RowPosition startPosition;
	private final RowPosition endPosition;
	private final IPartitioner<?> partitioner;
	private final Text[] keyAliasNames;
	private final Text[] columnAliasNames;
	private final ColumnFamilyMetadata metadata;
	private final RateLimiter rateLimiter;

	private final MapWritable key = new MapWritable();
	private final MapWritable value = new MapWritable();
	private final Map<ByteBuffer, Text> columnNamesMap = new HashMap<ByteBuffer, Text>();

	private FlattenIterator flattenIterator = null;

	private RangeRowIterator rowIterator;

	public HiveRowIterator(RowPosition startPosition, RowPosition endPosition, IPartitioner<?> partitioner, Text[] keyAliasNames, Text[] columnAliasNames,
			ColumnFamilyMetadata metadata, List<String> columnNames, RateLimiter rateLimiter) {
		this.startPosition = startPosition;
		this.endPosition = endPosition;
		this.partitioner = partitioner;
		this.keyAliasNames = keyAliasNames;
		this.columnAliasNames = columnAliasNames;
		this.metadata = metadata;
		this.rateLimiter = rateLimiter;

		// these are all the columns we will send back
		// columns that aren't here aren't required
		for (final String columnName : columnNames) {
			final ByteBuffer bb = CFDefinition.definitionType.fromString(columnName);
			final Text text = new Text(columnName);
			columnNamesMap.put(bb, text);
		}
	}

	RangeRowIterator createRangeRowIterator() throws IOException {
		return new RangeRowIterator(startPosition, endPosition, partitioner, metadata, rateLimiter);
	}

	@Override
	public boolean hasNext() {
		// if we're starting out
		if (rowIterator == null) {
			try {
				rowIterator = createRangeRowIterator();
			} catch (IOException e) {
				logger.error("Error starting RangeRowIterator.", e);
				throw new RuntimeException(e);
			}
		}
		if (flattenIterator != null && flattenIterator.hasNext())
			return true;
		flattenIterator = null;

		if (!rowIterator.hasNext())
			return false;

		// there may be nothing in the row we can return (e.g. just
		// tombstones)
		while (rowIterator.hasNext()) {
			Row row = rowIterator.next();
			flattenIterator = new FlattenIterator(row, keyAliasNames, columnAliasNames, metadata, columnNamesMap, key, value);
			if (flattenIterator.hasNext())
				return true;
		}

		return false;
	}

	@Override
	public Pair<MapWritable, MapWritable> next() {
		if (!hasNext())
			throw new NoSuchElementException();

		return flattenIterator.next();
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void close() throws IOException {
		rowIterator.close();
	}

}
