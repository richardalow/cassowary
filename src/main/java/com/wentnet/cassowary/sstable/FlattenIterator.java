package com.wentnet.cassowary.sstable;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.cassandra.cql3.CFDefinition;
import org.apache.cassandra.db.DeletedColumn;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

public class FlattenIterator implements Iterator<Pair<MapWritable, MapWritable>> {

	private static final Logger logger = LoggerFactory.getLogger(FlattenIterator.class);

	private final Text[] columnAliasNames;
	private final ColumnFamilyMetadata metadata;

	private final MapWritable keyComponentsMap;
	private final PeekingIterator<IColumn> columnIterator;

	private boolean collectedNext = true;
	private final MapWritable nextKey;
	private final MapWritable nextValue;
	private Pair<MapWritable, MapWritable> nextPair;
	// a map from ByteBuffer to the column name we return
	// columns not in here should not be returned
	private final Map<ByteBuffer, Text> columnNames;

	/**
	 * Pass in the key and value for efficiency, so they don't need to be
	 * created each time we move onto a new row.
	 * 
	 * @param row
	 * @param keyAliasNames
	 * @param columnAliasNames
	 * @param metadata
	 * @param key
	 * @param value
	 */
	public FlattenIterator(Row row, Text[] keyAliasNames, Text[] columnAliasNames, ColumnFamilyMetadata metadata, Map<ByteBuffer, Text> columnNames,
			MapWritable key, MapWritable value) {
		this.columnAliasNames = columnAliasNames;
		this.metadata = metadata;
		this.columnNames = columnNames;
		this.nextKey = key;
		this.nextValue = value;
		this.nextPair = Pair.create(nextKey, nextValue);

		logger.trace("Creating FlattenIterator for row {}.", row.key);

		ByteBuffer[] keyComponents;
		if (keyAliasNames.length > 1)
			keyComponents = ((CompositeType) metadata.getKeyValidator()).split(row.key.key);
		else
			keyComponents = new ByteBuffer[] { row.key.key };

		assert keyAliasNames.length == keyComponents.length;

		keyComponentsMap = new MapWritable();
		for (int i = 0; i < keyAliasNames.length; i++) {
			keyComponentsMap.put(keyAliasNames[i], new BytesWritable(ByteBufferUtil.getArray(keyComponents[i])));
		}

		columnIterator = Iterators.peekingIterator(row.cf.iterator());
	}

	@Override
	public boolean hasNext() {
		if (nextPair == null)
			return false;

		if (!collectedNext)
			return true;

		nextKey.clear();
		nextValue.clear();

		nextKey.putAll(keyComponentsMap);
		nextValue.putAll(keyComponentsMap);

		ByteBuffer[] columnKeyComponents = null;

		boolean addedColumns = false;

		while (columnIterator.hasNext()) {
			final IColumn col = columnIterator.peek();
			if (!(col instanceof DeletedColumn)) {
				final ByteBuffer[] columnComponents = ((CompositeType) metadata.getComparator()).split(col.name());

				assert columnComponents.length == columnAliasNames.length + 1;

				if (columnKeyComponents == null) {
					// use the first one as the key components
					columnKeyComponents = new ByteBuffer[columnComponents.length - 1];
					System.arraycopy(columnComponents, 0, columnKeyComponents, 0, columnComponents.length - 1);

					for (int i = 0; i < columnKeyComponents.length; i++) {
						final BytesWritable value = new BytesWritable(ByteBufferUtil.getArray(columnKeyComponents[i]));
						nextKey.put(columnAliasNames[i], value);
						nextValue.put(columnAliasNames[i], value);
					}

					// consume it since we're keeping it
					columnIterator.next();
				} else if (isSameColumnKeyComponent(columnKeyComponents, columnComponents)) {
					// keep it
					columnIterator.next();
				} else {
					// we got to the end of this part of the column key
					// components
					// must have consumed something so return happy
					assert addedColumns;
					collectedNext = false;
					return true;
				}

				final ByteBuffer nameBB = columnComponents[columnComponents.length - 1];
				// this will be null if this is a column we're not meant to
				// write
				final Text nameWritable = columnNames.get(nameBB);
				if (nameWritable != null) {
					final BytesWritable valWritable = new BytesWritable(ByteBufferUtil.getArray(col.value()));

					logger.debug("  Got column: {} -> {}", nameWritable, valWritable);

					nextValue.put(nameWritable, valWritable);
				} else if (logger.isDebugEnabled())
					logger.debug("  Skipping unrequired column: {}", CFDefinition.definitionType.getString(nameBB));
				// the map could still be empty, but at least we found a column
				// so the others
				// could all be null
				addedColumns = true;
			} else
				// consume it
				columnIterator.next();
		}

		// we got to the end
		// we may not have found any valid columns
		if (!addedColumns) {
			logger.debug("Flatten iterator didn't find any more columns.");
			nextPair = null;
			return false;
		}

		collectedNext = false;
		return true;
	}

	private boolean isSameColumnKeyComponent(ByteBuffer[] components1, ByteBuffer[] components2) {
		// work backwards since furthest right changes the most often
		for (int i = columnAliasNames.length - 1; i >= 0; i--) {
			if (!components1[i].equals(components2[i]))
				return false;
		}
		return true;
	}

	@Override
	public Pair<MapWritable, MapWritable> next() {
		if (!hasNext())
			throw new NoSuchElementException();

		collectedNext = true;
		return nextPair;
	}

	@Override
	public void remove() {
		throw new NoSuchElementException();
	}

}
