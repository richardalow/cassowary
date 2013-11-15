package com.wentnet.cassowary.sstable;

import java.util.List;
import java.util.Map;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.hadoop.mapred.JobConf;

public class ColumnFamilyMetadata {

	protected static final String KEY_ALIASES_KEY = "cassandra.cfmetadata.keyaliases";
	protected static final String COLUMN_ALIASES_KEY = "cassandra.cfmetadata.columnaliases";
	protected static final String KEY_VALIDATOR_KEY = "cassandra.cfmetadata.comparator";
	protected static final String COMPARATOR_KEY = "cassandra.cfmetadata.comparator";

	private final String keyspace;
	private final String columnFamily;
	private final String keyAliasesString;
	private final List<String> keyAliases;
	private final String columnAliasesString;
	private final List<String> columnAliases;
	private final String keyValidatorString;
	private final AbstractType<?> keyValidator;
	private final String comparatorString;
	private final AbstractType<?> comparator;

	public ColumnFamilyMetadata(String keyspace, String columnFamily, String keyAliasesString, String columnAliasesString, String keyValidatorString,
			String comparatorString) {
		// @formatter:off
		if (keyspace == null)            throw new IllegalArgumentException("keyspace cannot be null");
		if (columnFamily == null)        throw new IllegalArgumentException("columnFamily cannot be null");
		if (keyAliasesString == null)    throw new IllegalArgumentException("keyAliasesString cannot be null");
		if (columnAliasesString == null) throw new IllegalArgumentException("columnAliasesString cannot be null");
		if (keyValidatorString == null)  throw new IllegalArgumentException("keyValidatorString cannot be null");
		if (comparatorString == null)    throw new IllegalArgumentException("comparatorString cannot be null");
		// @formatter:on

		this.keyspace = keyspace;
		this.columnFamily = columnFamily;
		this.keyAliasesString = keyAliasesString;
		this.keyAliases = FBUtilities.fromJsonList(keyAliasesString);
		this.columnAliasesString = columnAliasesString;
		this.columnAliases = FBUtilities.fromJsonList(columnAliasesString);
		this.keyValidatorString = keyValidatorString;
		this.comparatorString = comparatorString;
		try {
			this.keyValidator = TypeParser.parse(keyValidatorString);
			this.comparator = TypeParser.parse(comparatorString);
		} catch (Exception e) {
			throw new IllegalArgumentException("Can't parse type string", e);
		}
	}

	public void writeToMap(Map<String, String> map) {
		map.put(KEY_ALIASES_KEY, keyAliasesString);
		map.put(COLUMN_ALIASES_KEY, columnAliasesString);
		map.put(KEY_VALIDATOR_KEY, keyValidatorString);
		map.put(COMPARATOR_KEY, comparatorString);
	}

	public static ColumnFamilyMetadata fromConf(String keyspace, String columnFamily, JobConf conf) {
		return new ColumnFamilyMetadata(keyspace, columnFamily, conf.get(KEY_ALIASES_KEY), conf.get(COLUMN_ALIASES_KEY), conf.get(KEY_VALIDATOR_KEY),
				conf.get(COMPARATOR_KEY));
	}

	public String getKeyspace() {
		return keyspace;
	}

	public String getColumnFamily() {
		return columnFamily;
	}

	public List<String> getKeyAliases() {
		return keyAliases;
	}

	public List<String> getColumnAliases() {
		return columnAliases;
	}

	public AbstractType<?> getComparator() {
		return comparator;
	}

	public AbstractType<?> getKeyValidator() {
		return keyValidator;
	}

	public CFMetaData toCFMetaData() {
		return new CFMetaData(keyspace, columnFamily, ColumnFamilyType.Standard, comparator, null);
	}

	@Override
	public String toString() {
		return "ColumnFamilyMetadata [keyspace=" + keyspace + ", columnFamily=" + columnFamily + ", keyAliasesString=" + keyAliasesString
				+ ", columnAliasesString=" + columnAliasesString + ", keyValidatorString=" + keyValidatorString + ", comparatorString=" + comparatorString
				+ "]";
	}
}
