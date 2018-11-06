package edu.xdu.psy.gplp.app;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/*
 * 
 * 
 * 
 * 自带分区编号的Id
 * 
 * 
 * 
 */

	public  class IdValue implements  WritableComparable{
		public static final String DELIMITER = "_";
		public short partition;
		public long id;

		public IdValue() {
		}

		public IdValue(String str) {
			String[] tokens = str.split(DELIMITER);
			this.id = Long.parseLong(tokens[0]);
			this.partition = Short.parseShort(tokens[1]);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			id = in.readLong();
			partition = in.readShort();
		
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeLong(id);
			out.writeShort(partition);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			IdValue other = (IdValue) o;
			if (this.partition == other.partition && this.id == other.id) {
				return true;
			}
			return false;
		}

		@Override
		public String toString() {
			return id + DELIMITER + partition;
		}

		@Override
		public int hashCode() {
			return (int) id;
		}

		public short getPartition() {
			return partition;
		}

		public long getId() {
			return id;
		}

		@Override
		public int compareTo(Object o) {
			if (o == this) {
				return 0;
			}
			IdValue other = (IdValue) o;
			return this.id > other.id ? +1 : this.id < other.id ? -1 : 0;
		}


	}