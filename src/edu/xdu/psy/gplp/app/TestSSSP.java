/**
 * Copyright 2014 Grafos.ml
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.xdu.psy.gplp.app;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.examples.LongDoubleFloatTextInputFormat;
import org.apache.giraph.io.EdgeInputFormat;
import org.apache.giraph.io.formats.GiraphFileInputFormat;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
import org.apache.giraph.io.formats.InMemoryVertexOutputFormat;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.PropertyConfigurator;

public class TestSSSP  implements Tool  {
	private Configuration conf;
	public static String filePath = "roadNet-PA_random.txt";
	public static String outputPath = "sssp";
	
	public static void main(String[] args) throws Exception {
		 PropertyConfigurator.configure("D:/config/log4j.properties");
		ToolRunner.run(new TestSSSP(), args);
}
	

	public int run(String[] args) throws Exception {
	
  	// 初始化config
		GiraphConfiguration giraphConf = new GiraphConfiguration(getConf());

		// 配置计算类
		giraphConf.setComputationClass(D_SSSP.class);
		//giraphConf.setEdgeInputFormatClass(EdgePartInputFormat.class);
		giraphConf.setEdgeInputFormatClass(OrginEdgeInputFormat.class);
		giraphConf.setVertexOutputFormatClass(IdWithValueTextOutputFormat.class);
		
		GiraphFileInputFormat.addEdgeInputPath(giraphConf,
				new Path(filePath));
			

		// 配置单机模式
		giraphConf.setLocalTestMode(true);
		giraphConf.setWorkerConfiguration(1, 1, 100);
		giraphConf.SPLIT_MASTER_WORKER.set(giraphConf, false);
		
		////////////////// 注意：worker数要跟文件一致！//////////////////
//		giraphConf.set("giraph.graphPartitionerFactoryClass", 
//		"com.cat.app.SpinnerPartitionerFactory");
		giraphConf.set("giraph.useSuperstepCounters","false");
		
		// 配置输出路径并开启job
	    InMemoryVertexOutputFormat. initializeOutputGraph(giraphConf);
		GiraphJob giraphJob = new GiraphJob(giraphConf, getClass().getName());
	      FileOutputFormat.setOutputPath(giraphJob. getInternalJob(), new Path(outputPath));
		giraphJob.run(true);
		return 0;
		
	}

	@Override
	public void setConf(Configuration conf) {
		// TODO Auto-generated method stub
		this.conf = conf;
	}


	@Override
	public Configuration getConf() {
		// TODO Auto-generated method stub
		return conf;
	}





}
