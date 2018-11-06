package edu.xdu.psy.gplp;

/**
 * @author psy
 * Test Main Method
 */

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.io.formats.GiraphFileInputFormat;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.PropertyConfigurator;

import edu.xdu.psy.gplp.GPLP.ConverterPropagate;
import edu.xdu.psy.gplp.GPLP.PartitionerMasterCompute;
import edu.xdu.psy.gplp.custom.GPLPEdgeInputFormat;
import edu.xdu.psy.gplp.custom.OpenHashMapEdges;

public class Test_GPLP implements Tool {
 
	//Gowalla_edges β=0.75 k=32 Edge Cut Rate (%)=45 Balance Rate (x1000)=1070 Total (ms)=518974
	
	private Configuration conf;
	public static String filePath = "Gowalla_edges.txt";
	public static String outPath = "out";
	

	public static void main(String[] args) throws Exception {
		 PropertyConfigurator.configure("D:/config/log4j.properties");
			ToolRunner.run(new Test_GPLP(), args);
	}
	

	public Configuration getConf() {
		// TODO Auto-generated method stub
		return conf;
	}

	public void setConf(Configuration conf) {
		// TODO Auto-generated method stub
		this.conf = conf;
	}

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		// String inputPath=arg0[0];
		// String outputPath=arg0[1];
	        String outputPath=outPath;

		// 初始化config
		GiraphConfiguration giraphConf = new GiraphConfiguration(getConf());

		// 配置计算类
		giraphConf.setComputationClass(ConverterPropagate.class);
		giraphConf.setMasterComputeClass(PartitionerMasterCompute.class);

		// 配置输入文件及输入数据格式
		giraphConf
				.setEdgeInputFormatClass(GPLPEdgeInputFormat.class);
		GiraphFileInputFormat.addEdgeInputPath(giraphConf,
				new Path(filePath));
		//facebook_combined

//		// //配置输出格式
		// giraphConf.setVertexOutputFormatClass(GPLPVertexValueOutputFormat.class);

		// 配置单机模式
		giraphConf.setLocalTestMode(true);
		giraphConf.setWorkerConfiguration(1, 1, 100);
		giraphConf.SPLIT_MASTER_WORKER.set(giraphConf, false);
		

		giraphConf.setOutEdgesClass(OpenHashMapEdges.class);
		// 配置输出路径并开启job
    	// InMemoryVertexOutputFormat. initializeOutputGraph(giraphConf);
		GiraphJob giraphJob = new GiraphJob(giraphConf, getClass().getName());
	    // FileOutputFormat.setOutputPath(giraphJob. getInternalJob(), new Path(outputPath));
		giraphJob.run(true);
		return 0;
	}

}
