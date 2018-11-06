package edu.xdu.psy.gplp.custom;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * 
 * @author psy
 * 自定义Message
 */

	public  class PartitionMessage implements Writable {
		private long sourceId;
		private short partition;

		public PartitionMessage() {
		}

		public PartitionMessage(long sourceId, short partition) {
			this.sourceId = sourceId;
			this.partition = partition;
		}

		public long getSourceId() {
			return sourceId;
		}

		public void setSourceId(long sourceId) {
			this.sourceId = sourceId;
		}

		public short getPartition() {
			return partition;
		}

		public void setPartition(short partition) {
			this.partition = partition;
		}

		@Override
		public void readFields(DataInput input) throws IOException {
			sourceId = input.readLong();
			partition = input.readShort();
		}

		@Override
		public void write(DataOutput output) throws IOException {
			output.writeLong(sourceId);
			output.writeShort(partition);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			PartitionMessage that = (PartitionMessage) o;
			if (partition != that.partition || sourceId != that.sourceId) {
				return false;
			}
			return true;
		}

		@Override
		public String toString() {
			return getSourceId() + " " + getPartition();
		}
	}