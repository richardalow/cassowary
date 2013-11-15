package com.wentnet.cassowary.sstable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;

public class SSTableSplit extends FileSplit implements InputSplit {

	private String startToken;
	private String endToken;
	private String columnMapping;
	private String[] hosts;
	private String keyspace;
	private String columnFamily;
	private long estimatedRows;
	private String partitioner;

	public SSTableSplit() {
		super((Path) null, 0, 0, (String[]) null);
	}

	public SSTableSplit(String columnsMapping, String startToken, String endToken, String[] hosts, Path dummyPath) {
		super(dummyPath, 0, 0, (String[]) null);
		this.columnMapping = columnsMapping;
		this.startToken = startToken;
		this.endToken = endToken;
		this.hosts = hosts;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		columnMapping = in.readUTF();
		startToken = in.readUTF();
		endToken = in.readUTF();
		final int numNodes = in.readInt();
		hosts = new String[numNodes];
		for (int i = 0; i < numNodes; i++)
			hosts[i] = in.readUTF();
		keyspace = in.readUTF();
		columnFamily = in.readUTF();
		estimatedRows = in.readLong();
		partitioner = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		out.writeUTF(columnMapping);
		out.writeUTF(startToken);
		out.writeUTF(endToken);
		out.writeInt(hosts.length);
		for (String host : hosts)
			out.writeUTF(host);
		out.writeUTF(keyspace);
		out.writeUTF(columnFamily);
		out.writeLong(estimatedRows);
		out.writeUTF(partitioner);
	}

	public String getStartToken() {
		return startToken;
	}

	public String getEndToken() {
		return endToken;
	}

	@Override
	public String[] getLocations() throws IOException {
		if (this.hosts == null) {
			return new String[] {};
		} else {
			return this.hosts;
		}
	}

	@Override
	public long getLength() {
		return 0;
	}

	public String getKeyspace() {
		return keyspace;
	}

	public void setKeyspace(String keyspace) {
		this.keyspace = keyspace;
	}

	public String getColumnFamily() {
		return columnFamily;
	}

	public void setColumnFamily(String columnFamily) {
		this.columnFamily = columnFamily;
	}

	public String getColumnMapping() {
		return columnMapping;
	}

	public void setColumnMapping(String mapping) {
		this.columnMapping = mapping;
	}

	public void setPartitioner(String part) {
		partitioner = part;
	}

	public String getPartitioner() {
		return partitioner;
	}

	@Override
	public String toString() {
		String[] locations;
		try {
			locations = getLocations();
		} catch (IOException e) {
			locations = new String[] { "unknown" };
		}
		return getPath().toString() + " [" + startToken + "," + endToken + ")@" + Arrays.toString(locations);
	}

	public void setEstimatedRows(long estimatedRows) {
		this.estimatedRows = estimatedRows;
	}

	public long getEstimatedRows() {
		return estimatedRows;
	}
}