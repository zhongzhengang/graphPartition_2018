package edu.xdu.psy.gplp;

/**
 * 
 * @author psy
 * 保存静态常量值
 *
 */

public class Const {
	public static final int DEFAULT_NUM_PARTITIONS = 64;
	public static final float DEFAULT_BALANCE_WEIGHT = 0.75f;
	public static final float DEFAULT_CONVERGENCE_THRESHOLD = 0.0025f;
	public static final float DEFAULT_ADDITIONAL_CAPACITY = 0.05f;
	public static final int DEFAULT_MAX_ITERATIONS = 90;
	public static final byte DEFAULT_EDGE_WEIGHT = 1;
	public static final int DEFAULT_WINDOW_SIZE = 5;
	public static final int DEFAULT_TS_SIZE = DEFAULT_NUM_PARTITIONS/2;
	public static final double DEFAULT_PROBABILITY = 0.2;
	
	/* default value backup
	public static final int DEFAULT_NUM_PARTITIONS = 16;
	public static final float DEFAULT_BALANCE_WEIGHT = 0.75f;
	public static final float DEFAULT_CONVERGENCE_THRESHOLD = 0.0025f;
	public static final float DEFAULT_ADDITIONAL_CAPACITY = 0.05f;
	public static final int DEFAULT_MAX_ITERATIONS = 90;
	public static final byte DEFAULT_EDGE_WEIGHT = 1;
	public static final int DEFAULT_WINDOW_SIZE = 5;
*/
	public static final String AGGREGATOR_LOAD_PREFIX = "AGG_LOAD_";
	public static final String AGGREGATOR_DEMAND_PREFIX = "AGG_DEMAND_";
	public static final String AGGREGATOR_STATE = "AGG_STATE";
	public static final String AGGREGATOR_MIGRATIONS = "AGG_MIGRATIONS";
	public static final String AGGREGATOR_CUTS = "AGG_CUTS";
	public static final String NUM_PARTITIONS = "gplp.numberOfPartitions";
	public static final String ADDITIONAL_CAPACITY = "gplp.additionalCapacity";
	public static final String MAX_ITERATIONS = "gplp.maxIterations";
	public static final String CONVERGENCE_THRESHOLD = "gplp.threshold";
	public static final String EDGE_WEIGHT = "gplp.weight";
	public static final String WINDOW_SIZE = "gplp.windowSize";
	public static final String COUNTER_GROUP = "Partitioning Counters";
	public static final String MIGRATIONS_COUNTER = "Migrations";
	public static final String ITERATIONS_COUNTER = "Iterations";
	public static final String PCT_CUT_EDGES_COUNTER = "Edge Cut Rate (%)";
	public static final String UNBALANCE_COUNTER = "Vertex/Edge Balance Rate (x1000)";
	public static final String BALANCE_WEIGHT = "gplp.balanceWeight";
}
