package com.wentnet.cassowary.sstable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableReader;

/**
 * This class manages all SSTableReaders. Every SSTableReader object consumes
 * some memory that is leaked unless the reader is freed. Unfortunately there is
 * no way to free it without deleting the file as well. This class will
 * therefore leak, but it guarantees to only open one reader per file. But over
 * time, if the Spark application is long lived this may become an issue.
 * 
 */
public final class SSTableManager {

	private static final SSTableManager theManager = new SSTableManager();

	private final ConcurrentHashMap<String, SSTableReader> readersMap = new ConcurrentHashMap<String, SSTableReader>();

	private SSTableManager() {
	}

	public static SSTableManager getManager() {
		return theManager;
	}

	public SSTableReader getReader(final String filename, CFMetaData cfMetadata) throws FileNotFoundException, IOException {
		SSTableReader reader = readersMap.get(filename);
		if (reader != null) {
			boolean acquired = reader.acquireReference();
			assert acquired;
			return reader;
		}

		// synchronize so we don't load a file more than once
		synchronized (this) {
			// now we have the lock, try again
			reader = readersMap.get(filename);
			if (reader != null) {
				boolean acquired = reader.acquireReference();
				assert acquired;
				return reader;
			}

			final Descriptor desc = Descriptor.fromFilename(filename);
			try {
				reader = SSTableReader.open(desc, cfMetadata);
				final SSTableReader previousReader = readersMap.put(filename, reader);
				assert previousReader == null;

				// get an extra reference so we never actually put them
				// since an SSTable gets deleted on put
				reader.acquireReference();
				return reader;
			} catch (Exception e) {
				// sadly we have to catch Exception since the
				// FileNotFoundException is wrapped

				if (e.getCause() instanceof FileNotFoundException)
					throw (FileNotFoundException) e.getCause();
				else
					throw new IOException(e);
			}
		}
	}

	public void putReader(final SSTableReader reader) {
		reader.releaseReference();
		// if we could properly release the reference then we could check to see
		// if we should remove it from the map
	}
}
