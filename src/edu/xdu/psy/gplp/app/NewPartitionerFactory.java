package edu.xdu.psy.gplp.app;

import org.apache.giraph.partition.GraphPartitionerFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

@SuppressWarnings("rawtypes")
public class NewPartitionerFactory<I extends WritableComparable,
V extends Writable, E extends Writable>
extends GraphPartitionerFactory<IdValue, V, E> {

	@Override
	public int getPartition(IdValue id, int partitionCount, int workerCount) {
		//////////////////////////////////////////////
		//System.out.println(id.getId()+" "+id.getPartition());
		/////////////////////////////////////////
		return id.getPartition();
	}

	@Override
	public int getWorker(int partition, int partitionCount, int workerCount) {
		// TODO Auto-generated method stub
		
		System.out.println(partition+" "+workerCount);
		 return partition % workerCount;
	}

	

}
