package com.wentnet.cassowary.sstable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.CharacterCodingException;
import java.util.Arrays;
import java.util.List;

import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.utils.Pair;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import com.google.common.util.concurrent.RateLimiter;
import com.wentnet.cassowary.sstable.ColumnFamilyMetadata;
import com.wentnet.cassowary.sstable.HiveRowIterator;
import com.wentnet.cassowary.sstable.RangeRowIterator;
import com.wentnet.cassowary.sstable.SSTableInputFormatImpl;

public class HiveRowIteratorTest {

	// @formatter:off
	/**
	 * This is for a class with schema and data
	 * 
	 * use ks;
	 * create table range_row_iterator_test (a text primary key, b text);
	 * insert into range_row_iterator_test (a, b) values ('a', '3');
	 * insert into range_row_iterator_test (a, b) values ('b', '4');
	 * insert into range_row_iterator_test (a, b) values ('c', '5');
	 * 
	 * The hash order of the rows is a, c, b
	 */
	// @formatter:on

	private static final String keyspace = "ks";
	private static final String columnFamily = "range_row_iterator_test";
	private static final String keyAliasesString = "[\"a\"]";
	private static final String columnAliasesString = "[]";
	private static final String keyValidatorString = "org.apache.cassandra.db.marshal.UTF8Type";
	private static final String comparatorString = "org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.UTF8Type)";

	// these are in the resources dir
	private static final String sstableFilePath = "/sstables/ks/range_row_iterator_test/ks-range_row_iterator_test-ic-1-Data.db";
	private static final String cassandraYamlPath = "cassandra.yaml";

	private static final Text[] keyAliasNames = { new Text("a") };
	private static final Text[] columnAliasNames = {};
	private static final List<String> columnNames = Arrays.asList("b");

	private IPartitioner<?> partitioner;
	private RateLimiter rateLimiter;
	private ColumnFamilyMetadata metadata;
	private File sstableFile;

	static {
		try {
			SSTableInputFormatImpl.setCassandraYamlLocation(Thread.currentThread().getContextClassLoader().getResource(cassandraYamlPath).getFile());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Before
	public void setup() {
		partitioner = new Murmur3Partitioner();
		rateLimiter = RateLimiter.create(1.0d * 1024.0d * 1024.0d);
		metadata = new ColumnFamilyMetadata(keyspace, columnFamily, keyAliasesString, columnAliasesString, keyValidatorString, comparatorString);
		sstableFile = new File(this.getClass().getResource(sstableFilePath).getPath());
	}

	@Test
	public void testWholeTable() throws IOException {

		final RowPosition startPosition = partitioner.getMinimumToken().minKeyBound();
		final RowPosition endPosition = startPosition;

		final RangeRowIterator spyRrIt = spy(new RangeRowIterator(startPosition, endPosition, partitioner, metadata, rateLimiter));
		doReturn(Arrays.asList(sstableFile)).when(spyRrIt).getSSTablesToRead();

		final HiveRowIterator spyHrIt = spy(new HiveRowIterator(startPosition, endPosition, partitioner, keyAliasNames, columnAliasNames, metadata,
				columnNames, rateLimiter));
		doReturn(spyRrIt).when(spyHrIt).createRangeRowIterator();

		assertTrue(spyHrIt.hasNext());
		assertRow("a", "3", spyHrIt.next());

		assertTrue(spyHrIt.hasNext());
		assertRow("c", "5", spyHrIt.next());

		assertTrue(spyHrIt.hasNext());
		assertRow("b", "4", spyHrIt.next());

		assertFalse(spyHrIt.hasNext());
	}

	protected static void assertRow(String a, String b, Pair<MapWritable, MapWritable> hiveRow) throws CharacterCodingException, UnsupportedEncodingException {
		final BytesWritable aValue = new BytesWritable(a.getBytes("UTF-8"));
		final Text aKey = new Text("a");
		final BytesWritable bValue = new BytesWritable(b.getBytes("UTF-8"));
		final Text bKey = new Text("b");

		assertEquals(1, hiveRow.left.size());
		assertEquals(aValue, hiveRow.left.get(aKey));

		assertEquals(2, hiveRow.right.size());
		assertEquals(aValue, hiveRow.right.get(aKey));
		assertEquals(bValue, hiveRow.right.get(bKey));
	}
}
