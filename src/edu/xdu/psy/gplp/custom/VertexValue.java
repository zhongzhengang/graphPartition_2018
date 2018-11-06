package edu.xdu.psy.gplp.custom;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.Writable;

import edu.xdu.psy.gplp.Const;

/**
 * 
 * @author mucao
 * 自定义VertexValue
 * 
 */
	public  class VertexValue implements Writable {
		private short currentPartition = -1;
		private short newPartition = -1;
		private LinkedHashSet<Integer> ts_list = new LinkedHashSet<>() ;//禁忌搜索列表
		
		public VertexValue() {
			
		}
		
		public boolean isExistedInTS(int p) {
			return ts_list.contains(p);
		}
		
		public boolean putPartition2TS(int p) {
			if(ts_list.size()>=Const.DEFAULT_TS_SIZE) {
				ts_list.remove(0);
			}
			return ts_list.add(p);
		}
		
		public short getCurrentPartition() {
			return currentPartition;
		}

		public void setCurrentPartition(short p) {
			currentPartition = p;
		}

		public short getNewPartition() {
			return newPartition;
		}

		public void setNewPartition(short p) {
			newPartition = p;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			currentPartition = in.readShort();
			newPartition = in.readShort();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeShort(currentPartition);
			out.writeShort(newPartition);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			VertexValue that = (VertexValue) o;
			if (currentPartition != that.currentPartition
					|| newPartition != that.newPartition) {
				return false;
			}
			return true;
		}

		@Override
		public String toString() {
			return getCurrentPartition() + " " + getNewPartition();
		}
	}
