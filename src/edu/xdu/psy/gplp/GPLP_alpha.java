package edu.xdu.psy.gplp;

import it.unimi.dsi.fastutil.shorts.ShortArrayList;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import org.apache.giraph.aggregators.DoubleSumAggregator;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import com.google.common.collect.Lists;

import edu.xdu.psy.gplp.custom.EdgeValue;
import edu.xdu.psy.gplp.custom.PartitionMessage;
import edu.xdu.psy.gplp.custom.VertexValue;

/**
 * 
 * @author psy
 * GPLP-α算法
 */

public class GPLP_alpha {
	public static boolean isVertexBalance; //true:设置平衡目标为点平衡；false：设置平衡目标为边平衡 
	public static float balanceWeight = 0.6f; //初始参数权重

	//设置动态自适应权重计数值
	private static int plusCount;
	private static int minusCount;
	
	/*********************** 预操作1 ：邻居传播 *************************/
	public static class ConverterPropagate
			extends
			AbstractComputation<LongWritable, VertexValue, EdgeValue, LongWritable, LongWritable> {

		// 把vertex ID 通过message发给邻居
		@Override
		public void compute(
				Vertex<LongWritable, VertexValue, EdgeValue> vertex,
				Iterable<LongWritable> messages) throws IOException {
			    sendMessageToAllEdges(vertex, vertex.getId());
		}
	}

	/***********************  预操作2：出边转换  *************************/
	public static class ConverterUpdateEdges
			extends
			AbstractComputation<LongWritable, VertexValue, EdgeValue, LongWritable, PartitionMessage> {
		private byte edgeWeight;
		@Override
		public void compute(
				Vertex<LongWritable, VertexValue, EdgeValue> vertex,
				Iterable<LongWritable> messages) throws IOException {
			for (LongWritable other : messages) {
				EdgeValue edgeValue = vertex.getEdgeValue(other);
				if (edgeValue == null) {
					edgeValue = new EdgeValue();
					edgeValue.setWeight((byte) 1);
					Edge<LongWritable, EdgeValue> edge = EdgeFactory.create(
							new LongWritable(other.get()), edgeValue);
					vertex.addEdge(edge);
				} else {
					edgeValue = new EdgeValue();
					edgeValue.setPartition((short) 0);
					edgeValue.setWeight(edgeWeight);
					vertex.setEdgeValue(other, edgeValue);
				}
			}
		}

		@Override
		public void preSuperstep() {
			edgeWeight = (byte) getContext().getConfiguration().getInt(
					Const.EDGE_WEIGHT, Const.DEFAULT_EDGE_WEIGHT);
		}
	}
	
	/*********************** 初始化操作3：初始标签设置 *************************/
	public static class InitPartition
			extends
			AbstractComputation<LongWritable, VertexValue, EdgeValue, PartitionMessage, PartitionMessage> {
		private String[] loadAggregatorNames;
		private int numberOfPartitions;
		private long totalVertexNum;

		private long totalEdgeNum;
		private long avgEdgeNum;
		private short partID = 0;
		private long partSize[];
		
		// 初划分：每个点随机赋一个分区
		@Override
		public void compute(
				Vertex<LongWritable, VertexValue, EdgeValue> vertex,
				Iterable<PartitionMessage> messages) throws IOException {
			short partition = vertex.getValue().getCurrentPartition(); // 获得当前分区
			if (partition == -1) {
				
				//统一使用chunk划分
			/*	partition = (short)(Math.ceil(vertex.getId().hashCode()/(((totalVertexNum + 1)/(numberOfPartitions + 0.0)))) - 1);
				if(partition == -1) {
					partition = 0;
				}
				aggregate(loadAggregatorNames[partition], new LongWritable(1));//统计分区partition中有多少个顶点
				aggregate(loadAggregatorNames[partition], new LongWritable(vertex.getNumEdges()));//统计partition分区中，有多少条边。
				*/
				
////////////////////////////负载平衡下的chunk划分。//////////////////////////////////////////////////
				if (isVertexBalance) {
					// 改为顶点划分
					partition = (short) (Math
						.ceil(vertex.getId().hashCode()	/ (((totalVertexNum + 1) / (numberOfPartitions + 0.0)))) - 1);
					if (partition == -1) {
						partition = 0;
					}	
					aggregate(loadAggregatorNames[partition], new LongWritable(1));//统计分区partition中有多少个顶点
					aggregate(loadAggregatorNames[partition], new LongWritable(vertex.getNumEdges()));//统计partition分区中，有多少条边。
				} else {
					// 负载平衡
					if (partSize[partID] <= avgEdgeNum) {
						partSize[partID] += vertex.getNumEdges();
						partition = partID;
						aggregate(loadAggregatorNames[partition],
								new LongWritable(vertex.getNumEdges())); // 计算这个分区，这个点有多少边
					} else if (partID < numberOfPartitions - 1) {
						partID++;
						if (partSize[partID] <= avgEdgeNum) {
							partSize[partID] += vertex.getNumEdges();
							partition = partID;
							aggregate(loadAggregatorNames[partition],
									new LongWritable(vertex.getNumEdges())); // 计算这个分区，这个点有多少边
						}
					}
				}

			}
			vertex.getValue().setCurrentPartition(partition);
			vertex.getValue().setNewPartition(partition);
			PartitionMessage message = new PartitionMessage(vertex.getId()
					.get(), partition);

			sendMessageToAllEdges(vertex, message);
		}

		@Override
		public void preSuperstep() {
			totalVertexNum = getTotalNumVertices();
			totalEdgeNum = getTotalNumEdges();
			numberOfPartitions = getContext().getConfiguration().getInt(
					Const.NUM_PARTITIONS, Const.DEFAULT_NUM_PARTITIONS);
			avgEdgeNum = totalEdgeNum/ numberOfPartitions;
			partSize = new long[numberOfPartitions];
			loadAggregatorNames = new String[numberOfPartitions];
			for (int i = 0; i < numberOfPartitions; i++) {
				loadAggregatorNames[i] = Const.AGGREGATOR_LOAD_PREFIX + i;
			}
		}
	}
	
	/*********************** 顶点迁移 ******************************/
	public static class VertexMigration
			extends
			AbstractComputation<LongWritable, VertexValue, EdgeValue, PartitionMessage, PartitionMessage> {
		private ShortArrayList maxIndices = new ShortArrayList();
		private int[] partitionFrequency;
		private long[] loads;
		private long totalCapacity;
		private short numberOfPartitions;
		private double additionalCapacity;
		private int count;
		private long expectCapacity;
		private String[] loadAggregatorNames;

		// 当前分区边数/最大预设分区边数
		private double computeW(int newPartition) {
			return (new BigDecimal(((double) loads[newPartition])
					/ totalCapacity).setScale(3, BigDecimal.ROUND_CEILING)
					.doubleValue());
		}

		/*
		 * 请求迁移到新分区 Request migration to a new partition
		 */
		private void requestMigration(
				Vertex<LongWritable, VertexValue, EdgeValue> vertex,
				int numberOfEdges, short currentPartition, short newPartition) {
			vertex.getValue().setNewPartition(newPartition); // 设置新区标签
			if (isVertexBalance) {
				loads[newPartition]++;
				loads[currentPartition]--; 
			} else {
				loads[newPartition] += numberOfEdges; // 新分区边++
				loads[currentPartition] -= numberOfEdges; // 旧分区边--
			}

		}

		/*
		 * 迁移后，更新邻居标签 Update the neighbor labels when they migrate
		 */
		private void updateNeighborsPartitions(
				Vertex<LongWritable, VertexValue, EdgeValue> vertex,
				Iterable<PartitionMessage> messages) { // message：sorceId
														// partition
			for (PartitionMessage message : messages) { // 迁移后的vertex
														// 的邻居的新message
				LongWritable otherId = new LongWritable(message.getSourceId()); // 邻居顶点id
				EdgeValue oldValue = vertex.getEdgeValue(otherId); // 邻居边
				/* 更新邻居标签 */
				vertex.setEdgeValue(
						otherId,
						new EdgeValue(message.getPartition(), oldValue
								.getWeight()));
			}
		}

		/*
		 * 计算邻居总标签和 Compute the occurrences of the labels in the neighborhood
		 * 
		 * 在这个函数完成过了三个功能：
		 * 1. 统计每个分区标签出现的频率（带有权重）
		 * 2. 统计所有的分区标签权重综合
		 * 3. 统计交互边数量
		 * 
		 * 注意一点，邻居的分区信息是放在顶点vertex的边上面的。
		 */
		private int computeNeighborsLabels(
				Vertex<LongWritable, VertexValue, EdgeValue> vertex) {
			Arrays.fill(partitionFrequency, 0); // 初始化数组，元素设0
			int totalLabels = 0;
			int cutEdges = 0;
			for (Edge<LongWritable, EdgeValue> e : vertex.getEdges()) { // 遍历vertex邻居
				partitionFrequency[e.getValue().getPartition()] += e.getValue() // 统计每个分区邻居边权重和
						.getWeight();
				totalLabels += e.getValue().getWeight(); // 统计vertex所有邻居权重和（总标签）
				if (e.getValue().getPartition() != vertex.getValue()// 如果vertex和邻居所在分区相同
						.getCurrentPartition()) {
					cutEdges++;// 交互边++
				}
			}
			// update cut edges stats
			aggregate(Const.AGGREGATOR_CUTS, new LongWritable(cutEdges));// 统计vertex的交互边数

			return totalLabels;
		}

		
//--------------------------------most important function------------------------------
		/*
		 * 根据邻居标签和当前分区边数来决定分区 Compute the new partition according to the
		 * neighborhood labels and the partitions' loads
		 */
		private short computeNewPartition_E_giraph(
				Vertex<LongWritable, VertexValue, EdgeValue> vertex,
				int totalLabels) {
			short currentPartition = vertex.getValue().getCurrentPartition();
			short newPartition = -1;
			double bestState = -Double.MAX_VALUE;
			double currentState = 0;
			maxIndices.clear();

			short maxPart = 0;
			long maxLoad = Long.MIN_VALUE;

			//下面这个for循环执行的就是依据顶点v的邻居信息和分区负载信息来确定要转移到哪个分区中去。
			for (short i = 0; i < numberOfPartitions ; i++) {
				if (loads[i] > maxLoad) {  //更新最大负载
					maxLoad = loads[i];
				}

				double PF = computeW(i); // 1*(当前分区边数/最大预设分区边数),：（0,1），越稳定越好   分区i的负载率
				double LPA = ((double) partitionFrequency[i]) / totalLabels; // vertex邻居在这个分区的总权重/vertex邻居总权重：（0,1），越大越要转移
//				double MORE = 1 - PF;  //分区i的剩余负载率。
//				double H = LPA * (1 - balanceWeight) + balanceWeight * MORE; // 越大越好    
				//(1 - balanceWeight)就相当于是alpha， balanceWeight相当于beta  
//				double H = LPA 
				double H = LPA * (1 - PF);


				if (i == currentPartition) {
					currentState = H; // 当前分区状态
				}

				/* 把最好状态的分区号保存 */
				if (H > bestState) {
					bestState = H;
					maxIndices.clear();
					maxIndices.add(i);// H比最好还好-->更新当前分区号
					maxPart = i;

				}

			}

			//动态自适应参数
			/*double maxNormal = (maxLoad + 0.0) / expectCapacity; // 平衡率
			double baseE = Math.log(0.5);
			if (getSuperstep() > 3) {
				if (maxNormal > (1 + additionalCapacity)) { // 如果不平衡-->增大平衡权重
					count++;
					if (count == 1) {
						plusCount++;
						double factor = (plusCount - 1) / 2.0;
						balanceWeight += (Math.exp(baseE - factor));
					}
				} else { // 否则减小平衡权重
					count++;
					if (count == 1) {
						minusCount++;
						double factor = (minusCount - 1) / 1.5;
						balanceWeight -= (Math.exp(baseE - factor));
					}
				}
			}*/
		
			newPartition = maxPart;
			// update state stats
			aggregate(Const.AGGREGATOR_STATE, new DoubleWritable(currentState));// 统计当前分区状态

			return newPartition;
		}
		
//--------------------------------most important function------------------------------
		/*
		 * 根据邻居标签和当前分区边数来决定分区 Compute the new partition according to the
		 * neighborhood labels and the partitions' loads
		 */
		private short computeNewPartition(
				Vertex<LongWritable, VertexValue, EdgeValue> vertex,
				int totalLabels) {
			short currentPartition = vertex.getValue().getCurrentPartition();
			short newPartition = -1;
			double bestState = -Double.MAX_VALUE;
			double currentState = 0;
			maxIndices.clear();

			short maxPart = 0;
			long maxLoad = Long.MIN_VALUE;

			//下面这个for循环执行的就是依据顶点v的邻居信息和分区负载信息来确定要转移到哪个分区中去。
			for (short i = 0; i < numberOfPartitions ; i++) {
				if (loads[i] > maxLoad) {  //更新最大负载
					maxLoad = loads[i];
				}

				double PF = computeW(i); // 1*(当前分区边数/最大预设分区边数),：（0,1），越稳定越好   分区i的负载率
				double LPA = ((double) partitionFrequency[i]) / totalLabels; // vertex邻居在这个分区的总权重/vertex邻居总权重：（0,1），越大越要转移
				double MORE = 1 - PF;  //分区i的剩余负载率。
				double H = LPA * (1 - balanceWeight) + balanceWeight * MORE; // 越大越好    
				//(1 - balanceWeight)就相当于是alpha， balanceWeight相当于beta  

				if (i == currentPartition) {
					currentState = H; // 当前分区状态
				}

				/* 把最好状态的分区号保存 */
				if (H > bestState) {
					bestState = H;
					maxIndices.clear();
					maxIndices.add(i);// H比最好还好-->更新当前分区号
					maxPart = i;

				}

			}

			//动态自适应参数
			double maxNormal = (maxLoad + 0.0) / expectCapacity; // 平衡率
			double baseE = Math.log(0.5);
			if (getSuperstep() > 3) {
				if (maxNormal > (1 + additionalCapacity)) { // 如果不平衡-->增大平衡权重
					count++;
					if (count == 1) {
						plusCount++;
						double factor = (plusCount - 1) / 2.0;
						balanceWeight += (Math.exp(baseE - factor));
					}
				} else { // 否则减小平衡权重
					count++;
					if (count == 1) {
						minusCount++;
						double factor = (minusCount - 1) / 1.5;
						balanceWeight -= (Math.exp(baseE - factor));

					}
				}
			}
		
			newPartition = maxPart;
			// update state stats
			aggregate(Const.AGGREGATOR_STATE, new DoubleWritable(currentState));// 统计当前分区状态

			return newPartition;
		}
		
		// 从旧分区迁移到新分区
		private void migrate (Vertex<LongWritable, VertexValue, EdgeValue> vertex,
				short currentPartition, short newPartition) {
			vertex.getValue().setCurrentPartition(newPartition);
			// update partitions loads
			int numberOfEdges = vertex.getNumEdges();

			if (isVertexBalance) {
				aggregate(loadAggregatorNames[currentPartition],
						new LongWritable(-1));
				aggregate(loadAggregatorNames[newPartition],
						new LongWritable(1));
			} else {
				aggregate(loadAggregatorNames[currentPartition],
						new LongWritable(-numberOfEdges)); // 老区边减
				aggregate(loadAggregatorNames[newPartition], new LongWritable(
						numberOfEdges)); // 新区边加
			}
			aggregate(Const.AGGREGATOR_MIGRATIONS, new LongWritable(1));
			// inform the neighbors
			PartitionMessage message = new PartitionMessage(vertex.getId()
					.get(), newPartition);
			sendMessageToAllEdges(vertex, message);// 把新分区的消息发送给vertex的每个邻居
			//正是因为在当前超步中顶点v在迁移的时候，向所有邻居顶点都发送了消息，所以在下一个超步中进行选择标签的时候，要先更新一下邻居顶点的信息。
		}



/////////////////////////被Giraph调用的计算步骤///////////////////////////////////////
		@Override
		public void compute(
				Vertex<LongWritable, VertexValue, EdgeValue> vertex,
				Iterable<PartitionMessage> messages) throws IOException {
			boolean isActive = messages.iterator().hasNext();
			short currentPartition = vertex.getValue().getCurrentPartition();
			int numberOfEdges = vertex.getNumEdges();

			// update neighbors partitions
			updateNeighborsPartitions(vertex, messages);

			// count labels occurrences in the neighborhood
			int totalLabels = computeNeighborsLabels(vertex);

			// compute the most attractive partition
//			short newPartition = computeNewPartition(vertex, totalLabels);
			short newPartition = computeNewPartition_E_giraph(vertex, totalLabels);

			// request migration to the new destination
			if (newPartition != currentPartition && isActive) {
				//禁忌列表
//				if(!vertex.getValue().isExistedInTS(newPartition)) {
					requestMigration(vertex, numberOfEdges, currentPartition,
							newPartition); //完成顶点在逻辑上的迁移，以及其他的附属操作
					migrate(vertex, currentPartition, newPartition);
					
					//vertex.getValue().putPartition2TS(currentPartition);//已经走过currentPartition这个分区了。
			
//				}				
			}
		}

		@Override
		public void preSuperstep() {
			additionalCapacity = getContext().getConfiguration().getFloat(
					Const.ADDITIONAL_CAPACITY, Const.DEFAULT_ADDITIONAL_CAPACITY);
			numberOfPartitions = (short) getContext().getConfiguration()
					.getInt(Const.NUM_PARTITIONS, Const.DEFAULT_NUM_PARTITIONS);
			partitionFrequency = new int[numberOfPartitions];
			loads = new long[numberOfPartitions];
			totalCapacity = isVertexBalance ? (long) Math
					.round((getTotalNumVertices()
							* (1 + additionalCapacity) / (numberOfPartitions )))
					: (long) Math
							.round((getTotalNumEdges()
									* (1 + additionalCapacity) / (numberOfPartitions )));
			expectCapacity = isVertexBalance ? (long) Math
					.round(((double) getTotalNumVertices() / (numberOfPartitions )))
					: (long) Math
							.round(((double) getTotalNumEdges() / (numberOfPartitions )));
					loadAggregatorNames = new String[numberOfPartitions];
					
			for (int i = 0; i < numberOfPartitions ; i++) {
				loadAggregatorNames[i] = Const.AGGREGATOR_LOAD_PREFIX + i;
				loads[i] = ((LongWritable) getAggregatedValue(Const.AGGREGATOR_LOAD_PREFIX
						+ i)).get();
			}
	
		}
	}
	
	/*********************** 顶点迁移 ******************************/
	public static class VertexMigrationDisturbance
			extends
			AbstractComputation<LongWritable, VertexValue, EdgeValue, PartitionMessage, PartitionMessage> {
		private ShortArrayList maxIndices = new ShortArrayList();
		private int[] partitionFrequency;
		private long[] loads;
		private long totalCapacity;
		private short numberOfPartitions;
		private double additionalCapacity;
		private int count;
		private long expectCapacity;
		private String[] loadAggregatorNames;
		private double disturbanceProbability = Const.DEFAULT_PROBABILITY;
		
		
		// 当前分区边数/最大预设分区边数
		private double computeW(int newPartition) {
			return (new BigDecimal(((double) loads[newPartition])
					/ totalCapacity).setScale(3, BigDecimal.ROUND_CEILING)
					.doubleValue());
		}

		/*
		 * 请求迁移到新分区 Request migration to a new partition
		 */
		private void requestMigration(
				Vertex<LongWritable, VertexValue, EdgeValue> vertex,
				int numberOfEdges, short currentPartition, short newPartition) {
			vertex.getValue().setNewPartition(newPartition); // 设置新区标签
			if (isVertexBalance) {
				loads[newPartition]++;
				loads[currentPartition]--; 
			} else {
				loads[newPartition] += numberOfEdges; // 新分区边++
				loads[currentPartition] -= numberOfEdges; // 旧分区边--
			}

		}

		/*
		 * 迁移后，更新邻居标签 Update the neighbor labels when they migrate
		 */
		private void updateNeighborsPartitions(
				Vertex<LongWritable, VertexValue, EdgeValue> vertex,
				Iterable<PartitionMessage> messages) { // message：sorceId
														// partition
			for (PartitionMessage message : messages) { // 迁移后的vertex
														// 的邻居的新message
				LongWritable otherId = new LongWritable(message.getSourceId()); // 邻居顶点id
				EdgeValue oldValue = vertex.getEdgeValue(otherId); // 邻居边
				/* 更新邻居标签 */
				vertex.setEdgeValue(
						otherId,
						new EdgeValue(message.getPartition(), oldValue
								.getWeight()));
			}
		}

		/*
		 * 计算邻居总标签和 Compute the occurrences of the labels in the neighborhood
		 * 
		 * 在这个函数完成过了三个功能：
		 * 1. 统计每个分区标签出现的频率（带有权重）
		 * 2. 统计所有的分区标签权重综合
		 * 3. 统计交互边数量
		 * 
		 * 注意一点，邻居的分区信息是放在顶点vertex的边上面的。
		 */
		private int computeNeighborsLabels(
				Vertex<LongWritable, VertexValue, EdgeValue> vertex) {
			Arrays.fill(partitionFrequency, 0); // 初始化数组，元素设0
			int totalLabels = 0;
			int cutEdges = 0;
			for (Edge<LongWritable, EdgeValue> e : vertex.getEdges()) { // 遍历vertex邻居
				partitionFrequency[e.getValue().getPartition()] += e.getValue() // 统计每个分区邻居边权重和
						.getWeight();
				totalLabels += e.getValue().getWeight(); // 统计vertex所有邻居权重和（总标签）
				if (e.getValue().getPartition() != vertex.getValue()// 如果vertex和邻居所在分区相同
						.getCurrentPartition()) {
					cutEdges++;// 交互边++
				}
			}
			// update cut edges stats
			aggregate(Const.AGGREGATOR_CUTS, new LongWritable(cutEdges));// 统计vertex的交互边数

			return totalLabels;
		}

		
//--------------------------------most important function------------------------------
		/*
		 * 根据邻居标签和当前分区边数来决定分区 Compute the new partition according to the
		 * neighborhood labels and the partitions' loads
		 */
		private short computeNewPartition_E_giraph(
				Vertex<LongWritable, VertexValue, EdgeValue> vertex,
				int totalLabels) {
			short currentPartition = vertex.getValue().getCurrentPartition();
			short newPartition = -1;
			double bestState = -Double.MAX_VALUE;
			double currentState = 0;
			maxIndices.clear();

			short maxPart = 0;
			long maxLoad = Long.MIN_VALUE;

			//下面这个for循环执行的就是依据顶点v的邻居信息和分区负载信息来确定要转移到哪个分区中去。
			for (short i = 0; i < numberOfPartitions ; i++) {
				if (loads[i] > maxLoad) {  //更新最大负载
					maxLoad = loads[i];
				}

				double PF = computeW(i); // 1*(当前分区边数/最大预设分区边数),：（0,1），越稳定越好   分区i的负载率
				double LPA = ((double) partitionFrequency[i]) / totalLabels; // vertex邻居在这个分区的总权重/vertex邻居总权重：（0,1），越大越要转移
//				double MORE = 1 - PF;  //分区i的剩余负载率。
//				double H = LPA * (1 - balanceWeight) + balanceWeight * MORE; // 越大越好    
				//(1 - balanceWeight)就相当于是alpha， balanceWeight相当于beta  
				double H = LPA * (1 - PF);				

				
				if (i == currentPartition) {
					currentState = H; // 当前分区状态
				}

				/* 把最好状态的分区号保存 */
				if (H > bestState) {
					bestState = H;
					maxIndices.clear();
					maxIndices.add(i);// H比最好还好-->更新当前分区号
					maxPart = i;

				}

			}

			//动态自适应参数
			/*double maxNormal = (maxLoad + 0.0) / expectCapacity; // 平衡率
			double baseE = Math.log(0.5);
			if (getSuperstep() > 3) {
				if (maxNormal > (1 + additionalCapacity)) { // 如果不平衡-->增大平衡权重
					count++;
					if (count == 1) {
						plusCount++;
						double factor = (plusCount - 1) / 2.0;
						balanceWeight += (Math.exp(baseE - factor));
					}
				} else { // 否则减小平衡权重
					count++;
					if (count == 1) {
						minusCount++;
						double factor = (minusCount - 1) / 1.5;
						balanceWeight -= (Math.exp(baseE - factor));

					}
				}
			}*/
		
			newPartition = maxPart;
			// update state stats
			aggregate(Const.AGGREGATOR_STATE, new DoubleWritable(currentState));// 统计当前分区状态

			return newPartition;
		}
		
//--------------------------------most important function------------------------------
		/*
		 * 根据邻居标签和当前分区边数来决定分区 Compute the new partition according to the
		 * neighborhood labels and the partitions' loads
		 */
		private short computeNewPartition(
				Vertex<LongWritable, VertexValue, EdgeValue> vertex,
				int totalLabels) {
			short currentPartition = vertex.getValue().getCurrentPartition();
			short newPartition = -1;
			double bestState = -Double.MAX_VALUE;
			double currentState = 0;
			maxIndices.clear();

			short maxPart = 0;
			long maxLoad = Long.MIN_VALUE;

			//下面这个for循环执行的就是依据顶点v的邻居信息和分区负载信息来确定要转移到哪个分区中去。
			for (short i = 0; i < numberOfPartitions ; i++) {
				if (loads[i] > maxLoad) {  //更新最大负载
					maxLoad = loads[i];
				}

				double PF = computeW(i); // 1*(当前分区边数/最大预设分区边数),：（0,1），越稳定越好   分区i的负载率
				double LPA = ((double) partitionFrequency[i]) / totalLabels; // vertex邻居在这个分区的总权重/vertex邻居总权重：（0,1），越大越要转移
				double MORE = 1 - PF;  //分区i的剩余负载率。
				double H = LPA * (1 - balanceWeight) + balanceWeight * MORE; // 越大越好    
				//(1 - balanceWeight)就相当于是alpha， balanceWeight相当于beta  

				if (i == currentPartition) {
					currentState = H; // 当前分区状态
				}

				/* 把最好状态的分区号保存 */
				if (H > bestState) {
					bestState = H;
					maxIndices.clear();
					maxIndices.add(i);// H比最好还好-->更新当前分区号
					maxPart = i;

				}

			}

			//动态自适应参数
			double maxNormal = (maxLoad + 0.0) / expectCapacity; // 平衡率
			double baseE = Math.log(0.5);
			if (getSuperstep() > 3) {
				if (maxNormal > (1 + additionalCapacity)) { // 如果不平衡-->增大平衡权重
					count++;
					if (count == 1) {
						plusCount++;
						double factor = (plusCount - 1) / 2.0;
						balanceWeight += (Math.exp(baseE - factor));
					}
				} else { // 否则减小平衡权重
					count++;
					if (count == 1) {
						minusCount++;
						double factor = (minusCount - 1) / 1.5;
						balanceWeight -= (Math.exp(baseE - factor));

					}
				}
			}
		
			newPartition = maxPart;
			// update state stats
			aggregate(Const.AGGREGATOR_STATE, new DoubleWritable(currentState));// 统计当前分区状态

			return newPartition;
		}
		
		// 从旧分区迁移到新分区
		private void migrate (Vertex<LongWritable, VertexValue, EdgeValue> vertex,
				short currentPartition, short newPartition) {
			vertex.getValue().setCurrentPartition(newPartition);
			// update partitions loads
			int numberOfEdges = vertex.getNumEdges();

			if (isVertexBalance) {
				aggregate(loadAggregatorNames[currentPartition],
						new LongWritable(-1));
				aggregate(loadAggregatorNames[newPartition],
						new LongWritable(1));
			} else {
				aggregate(loadAggregatorNames[currentPartition],
						new LongWritable(-numberOfEdges)); // 老区边减
				aggregate(loadAggregatorNames[newPartition], new LongWritable(
						numberOfEdges)); // 新区边加
			}
			aggregate(Const.AGGREGATOR_MIGRATIONS, new LongWritable(1));
			// inform the neighbors
			PartitionMessage message = new PartitionMessage(vertex.getId()
					.get(), newPartition);
			sendMessageToAllEdges(vertex, message);// 把新分区的消息发送给vertex的每个邻居
			//正是因为在当前超步中顶点v在迁移的时候，向所有邻居顶点都发送了消息，所以在下一个超步中进行选择标签的时候，要先更新一下邻居顶点的信息。
		}



/////////////////////////被Giraph调用的计算步骤///////////////////////////////////////
		@Override
		public void compute(
				Vertex<LongWritable, VertexValue, EdgeValue> vertex,
				Iterable<PartitionMessage> messages) throws IOException {
			boolean isActive = messages.iterator().hasNext();
			short currentPartition = vertex.getValue().getCurrentPartition();
			int numberOfEdges = vertex.getNumEdges();

			// update neighbors partitions
			updateNeighborsPartitions(vertex, messages);

			// count labels occurrences in the neighborhood
			int totalLabels = computeNeighborsLabels(vertex);

			// compute the most attractive partition
//			short newPartition = computeNewPartition(vertex, totalLabels);
			short newPartition = computeNewPartition_E_giraph(vertex, totalLabels);

			// request migration to the new destination
			if (newPartition != currentPartition && isActive) {
				//引入概率，判断是否要转移
				if(Math.random() < disturbanceProbability) {
					if(!vertex.getValue().isExistedInTS(newPartition)) {
						requestMigration(vertex, numberOfEdges, currentPartition,
								newPartition); //完成顶点在逻辑上的迁移，以及其他的附属操作
						migrate(vertex, currentPartition, newPartition);
						vertex.getValue().putPartition2TS(currentPartition);
					}
				}
			}
		}

		@Override
		public void preSuperstep() {
			additionalCapacity = getContext().getConfiguration().getFloat(
					Const.ADDITIONAL_CAPACITY, Const.DEFAULT_ADDITIONAL_CAPACITY);
			numberOfPartitions = (short) getContext().getConfiguration()
					.getInt(Const.NUM_PARTITIONS, Const.DEFAULT_NUM_PARTITIONS);
			partitionFrequency = new int[numberOfPartitions];
			loads = new long[numberOfPartitions];
			totalCapacity = isVertexBalance ? (long) Math
					.round((getTotalNumVertices()
							* (1 + additionalCapacity) / (numberOfPartitions )))
					: (long) Math
							.round((getTotalNumEdges()
									* (1 + additionalCapacity) / (numberOfPartitions )));
			expectCapacity = isVertexBalance ? (long) Math
					.round(((double) getTotalNumVertices() / (numberOfPartitions )))
					: (long) Math
							.round(((double) getTotalNumEdges() / (numberOfPartitions )));
					loadAggregatorNames = new String[numberOfPartitions];
					
			for (int i = 0; i < numberOfPartitions ; i++) {
				loadAggregatorNames[i] = Const.AGGREGATOR_LOAD_PREFIX + i;
				loads[i] = ((LongWritable) getAggregatedValue(Const.AGGREGATOR_LOAD_PREFIX
						+ i)).get();
			}
	
		}
	}
	
	
	/*********************** 主计算类：控制各超步计算类顺序以及判断迭代收敛  *************************/
	public static class PartitionerMasterCompute extends DefaultMasterCompute {
		private LinkedList<Double> states;
		private String[] loadAggregatorNames;
		private int maxIterations;
		private int numberOfPartitions;
		private double convergenceThreshold;
		private int windowSize; //滑动窗口长度
		private long totalMigrations;
		private double maxNormLoad;
		/////////////////////////  E_Giraph    ///////////////////////////
		private boolean isDisturbance; //是否进行扰动
		private int dis_count; 
		private int ld_superStep ;//上一次进行扰动时的超步。
		
		@Override
		public void initialize() throws InstantiationException,
				IllegalAccessException {
			maxIterations = getContext().getConfiguration().getInt(
					Const.MAX_ITERATIONS, Const.DEFAULT_MAX_ITERATIONS);
			numberOfPartitions = getContext().getConfiguration().getInt(
					Const.NUM_PARTITIONS, Const.DEFAULT_NUM_PARTITIONS);
			convergenceThreshold = getContext().getConfiguration().getFloat(
					Const.CONVERGENCE_THRESHOLD, Const.DEFAULT_CONVERGENCE_THRESHOLD);
			windowSize = getContext().getConfiguration().getInt(
					Const.WINDOW_SIZE, Const.DEFAULT_WINDOW_SIZE);
			states = Lists.newLinkedList();
			// Create aggregators for each partition
			loadAggregatorNames = new String[numberOfPartitions];
			for (int i = 0; i < numberOfPartitions ; i++) {
				loadAggregatorNames[i] = Const.AGGREGATOR_LOAD_PREFIX + i;
				registerPersistentAggregator(loadAggregatorNames[i],
						LongSumAggregator.class);
				registerAggregator(Const.AGGREGATOR_DEMAND_PREFIX + i,
						LongSumAggregator.class);
			}
			registerAggregator(Const.AGGREGATOR_STATE, DoubleSumAggregator.class);
			registerAggregator(Const.AGGREGATOR_CUTS, LongSumAggregator.class);
			registerAggregator(Const.AGGREGATOR_MIGRATIONS, LongSumAggregator.class);
		}
		// /////////////////////////收敛条件////////////////////////////
		private boolean algorithmConverged(int superstep) {
			double additionalCapacity = getContext().getConfiguration()
					.getFloat(Const.ADDITIONAL_CAPACITY, Const.DEFAULT_ADDITIONAL_CAPACITY);
			long cutEdges = ((LongWritable) getAggregatedValue(Const.AGGREGATOR_CUTS))
					.get();
			
			boolean converged = false;
		
				double edgeCutRate = ((double) cutEdges) / getTotalNumEdges();
				long maxLoad = -Long.MAX_VALUE;
				for (int i = 0; i < numberOfPartitions ; i++) {
					long load = ((LongWritable) getAggregatedValue(loadAggregatorNames[i]))
							.get();
					
					if (load > maxLoad) {
						maxLoad = load;
					}
				}
				//如果isVertexBalance为true，那么这里的maxLoad统计出来的就是每个分区中最大顶点数；如果isVertexBalance为false，那么这里的maxLoad统计出来的就是每个分区中最大顶点数
				double expectedLoad = isVertexBalance ? ((double) getTotalNumVertices())
						/ (numberOfPartitions )
						: ((double) getTotalNumEdges())
								/ (numberOfPartitions );
				double maxNormal = ((maxLoad) / expectedLoad);
		
				double newState = edgeCutRate;
				if (superstep > 3 + windowSize) {
					double best = Collections.max(states);
					double step = Math.abs(1 - newState / best);

					if (maxNormal < ((additionalCapacity + 1) + 0.01)
							&& step < convergenceThreshold) {
						converged = true;
					}

					states.removeFirst();//将窗口中旧的状态移除掉
				}
				states.addLast(newState);//将当前状态添加到窗口的最前面
		
	
			return converged;
		}

		private void setCounters() {
			long localEdges = ((LongWritable) getAggregatedValue(Const.AGGREGATOR_CUTS))
					.get();
			long localEdgesPct = (long) (100 * ((double) localEdges) / getTotalNumEdges());
			getContext().getCounter(Const.COUNTER_GROUP, Const.MIGRATIONS_COUNTER)
					.increment(totalMigrations);
			getContext().getCounter(Const.COUNTER_GROUP, Const.ITERATIONS_COUNTER)
					.increment(getSuperstep());
			getContext().getCounter(Const.COUNTER_GROUP, Const.PCT_CUT_EDGES_COUNTER)
					.increment(localEdgesPct);
			getContext().getCounter(Const.COUNTER_GROUP,
					Const.UNBALANCE_COUNTER).increment(
					(long) (1000 * maxNormLoad));
		}

		private void updateStats() {
			totalMigrations += ((LongWritable) getAggregatedValue(Const.AGGREGATOR_MIGRATIONS))
					.get();
			long maxLoad = -Long.MAX_VALUE;
			for (int i = 0; i < numberOfPartitions ; i++) {
				long load = ((LongWritable) getAggregatedValue(loadAggregatorNames[i]))
						.get();
				if (load > maxLoad) {
					maxLoad = load;
				}
			}
		
			double expectedLoad = isVertexBalance ? ((double) getTotalNumVertices())
					/ (numberOfPartitions )
					: ((double) getTotalNumEdges())
							/ (numberOfPartitions );
			maxNormLoad = (maxLoad) / expectedLoad;
		}

		// 根据当前超步决定调用哪个步骤的计算
		@Override
		public void compute() {
			int superstep = (int) getSuperstep();
			
			if(isDisturbance) {//进行扰动
				
				setComputation(VertexMigrationDisturbance.class);
				isDisturbance = false;//扰动之后，下一次不进行扰动。
				
			}else {//不进行扰动
				if (superstep == 0) {
					setComputation(ConverterPropagate.class);
				} else if (superstep == 1) {
					setComputation(ConverterUpdateEdges.class);
				} else if (superstep == 2) {
						setComputation(InitPartition.class);
				} else {
						setComputation(VertexMigration.class);
				}
			}
			
			boolean hasConverged = false;
			if (superstep > 3) {
					hasConverged = algorithmConverged(superstep);
					//如果达到收敛条件，则进行一次扰动
					if(hasConverged && superstep < maxIterations) {
						isDisturbance = true;
						//记录此次收敛时的结果
						dis_count++;
						
						if(dis_count > 1) {
							if((superstep - ld_superStep)>2*windowSize) {
								hasConverged = false;
							}
						}
						ld_superStep = superstep;
					}
			}
			updateStats(); //对maxNormLoad进行更新
			
			//整个算法停止条件
			if ((hasConverged) || superstep >= maxIterations) {
				System.out.println("Halting computation: " + hasConverged);
				haltComputation();
				setCounters();
			}
		}
	}
}
