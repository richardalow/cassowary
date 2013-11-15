package com.wentnet.cassowary.sstable;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableScanner;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.MergeIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.common.util.concurrent.RateLimiter;

public class RangeRowIterator implements CloseableIterator<Row> {

	private static final Logger logger = LoggerFactory.getLogger(RangeRowIterator.class);

	private final RowPosition startPosition;
	private final RowPosition endPosition;
	private final ColumnFamilyMetadata metadata;
	private final CFMetaData cfMetadata;
	private final RateLimiter rateLimiter;
	private final boolean isWrapAround;
	private final RowPosition minPosition;

	// if this range is wrap around, this is true when we wrap round to
	// minPosition
	private boolean hasWrapped = false;
	// set if we got to the end or were closed
	private boolean finished = false;

	private final List<SSTableScanner> scanners = new ArrayList<SSTableScanner>();
	private final List<SSTableReader> readers = new ArrayList<SSTableReader>();
	private CloseableIterator<Row> rowIterator;
	private PeekingIterator<Row> peekingRowIterator;

	public RangeRowIterator(RowPosition startPosition, RowPosition endPosition, IPartitioner<?> partitioner, ColumnFamilyMetadata metadata,
			RateLimiter rateLimiter) throws IOException {
		this.startPosition = startPosition;
		this.endPosition = endPosition;
		this.metadata = metadata;
		this.cfMetadata = metadata.toCFMetaData();
		this.rateLimiter = rateLimiter;

		minPosition = partitioner.getMinimumToken().minKeyBound();

		isWrapAround = startPosition.compareTo(endPosition) >= 0;
	}

	private CloseableIterator<Row> getRowIterator(RowPosition start) throws IOException {
		releaseReferences();
		// loop round until we get a successful read
		List<CloseableIterator<OnDiskAtomIterator>> iterators = null;
		while (iterators == null)
			iterators = getSSTableIterators(start);

		return makeRowIterator(iterators);
	}

	List<File> getSSTablesToRead() {
		final Directories directories = Directories.create(metadata.getKeyspace(), metadata.getColumnFamily());
		final Directories.SSTableLister sstableFiles = directories.sstableLister().skipTemporary(true);

		return sstableFiles.listFiles();
	}

	/**
	 * Gets a list of iterators. Will return null if a file is removed while we
	 * are loading. This will happen if a compaction completes while we are in
	 * this function. Throws IOException if anything else goes wrong.
	 * 
	 * @param start
	 * @return
	 * @throws IOException
	 */
	private List<CloseableIterator<OnDiskAtomIterator>> getSSTableIterators(RowPosition start) throws IOException {
		final List<CloseableIterator<OnDiskAtomIterator>> iterators = new ArrayList<CloseableIterator<OnDiskAtomIterator>>();

		for (File sstableFile : getSSTablesToRead()) {
			final String sstablePath = sstableFile.getAbsolutePath();

			// NB if we add in the other files here is is much slower to load
			if (sstablePath.endsWith("-Data.db")) {
				logger.debug("Adding file {} ", sstableFile);

				try {
					final SSTableReader reader = SSTableManager.getManager().getReader(sstableFile.getAbsolutePath(), cfMetadata);
					readers.add(reader);

					// this already sets it to not cache
					final SSTableScanner scanner = reader.getDirectScanner(rateLimiter);
					scanners.add(scanner);
					scanner.seekTo(start);
					iterators.add(scanner);
				} catch (FileNotFoundException e) {
					// we may get FileNotFoundException from
					// SSTableManager.getReader or wrapped from
					// reader.getDirectScanner
					// which is caught below
					releaseReferences();
					logger.info("SStable {} disappeared while reading, starting over", sstableFile);
					return null;
				} catch (Exception e) {
					releaseReferences();
					if (e.getCause() instanceof FileNotFoundException) {
						logger.info("SStable {} disappeared while reading, starting over", sstableFile);
						return null;
					}
					releaseReferences();
					throw new IOException(e);
				}
			}
		}
		return iterators;
	}

	private void releaseReferences() {
		for (final SSTableReader reader : readers) {
			try {
				SSTableManager.getManager().putReader(reader);
			} catch (Exception e) {
				logger.error("Error releasing reference for reader.", e);
			}
		}
		readers.clear();

		try {
			FileUtils.close(scanners);
		} catch (Exception e) {
			logger.error("Error closing scanners.", e);
		}
		scanners.clear();
	}

	private static final Comparator<OnDiskAtomIterator> COMPARE_BY_KEY = new Comparator<OnDiskAtomIterator>() {
		public int compare(OnDiskAtomIterator o1, OnDiskAtomIterator o2) {
			return DecoratedKey.comparator.compare(o1.getKey(), o2.getKey());
		}
	};

	private CloseableIterator<Row> makeRowIterator(List<CloseableIterator<OnDiskAtomIterator>> iterators) {
		// reduce rows from all sources into a single row
		return MergeIterator.get(iterators, COMPARE_BY_KEY, new MergeIterator.Reducer<OnDiskAtomIterator, Row>() {
			private final List<OnDiskAtomIterator> colIters = new ArrayList<OnDiskAtomIterator>();
			private DecoratedKey key;
			private ColumnFamily returnCF;
			private final QueryFilter filter = QueryFilter.getIdentityFilter(null, new QueryPath(metadata.getColumnFamily()));

			@Override
			protected void onKeyChange() {
				this.returnCF = ColumnFamily.create(cfMetadata);
			}

			@Override
			public void reduce(OnDiskAtomIterator current) {
				this.colIters.add(current);
				this.key = current.getKey();
				this.returnCF.delete(current.getColumnFamily());
			}

			@Override
			protected Row getReduced() {
				filter.collateOnDiskAtom(returnCF, colIters, Integer.MAX_VALUE);

				Row rv = new Row(key, returnCF);
				colIters.clear();
				key = null;
				return rv;
			}
		});
	}

	@Override
	public boolean hasNext() {
		if (finished)
			return false;

		// we're starting
		if (peekingRowIterator == null) {
			try {
				rowIterator = getRowIterator(startPosition);
			} catch (IOException e) {
				logger.error("Error getting initial row iterator", e);
				throw new RuntimeException(e);
			}
			peekingRowIterator = Iterators.peekingIterator(rowIterator);
		}

		if (!peekingRowIterator.hasNext()) {
			// if wrap around, start at min token and finish at end
			if (isWrapAround) {
				try {
					rowIterator.close();
					rowIterator = getRowIterator(minPosition);
					peekingRowIterator = Iterators.peekingIterator(rowIterator);
				} catch (IOException e) {
					logger.error("Error getting next row iterator", e);
					throw new RuntimeException(e);
				}
				hasWrapped = true;
				if (!peekingRowIterator.hasNext()) {
					close();
					return false;
				}
			} else {
				close();
				return false;
			}
		}

		Row row = peekingRowIterator.peek();
		// exclusive upper range
		if (!isWrapAround || (isWrapAround && hasWrapped)) {
			if (row.key.compareTo(endPosition) >= 0) {
				close();
				return false;
			}
		}

		return true;
	}

	@Override
	public Row next() {
		if (!hasNext())
			throw new NoSuchElementException();

		return peekingRowIterator.next();
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void close() {
		finished = true;
		releaseReferences();
		if (rowIterator != null) {
			try {
				rowIterator.close();
			} catch (IOException e) {
				logger.error("Error closing row iterator", e);
			}
		}
		rowIterator = null;
		peekingRowIterator = null;
	}

}
