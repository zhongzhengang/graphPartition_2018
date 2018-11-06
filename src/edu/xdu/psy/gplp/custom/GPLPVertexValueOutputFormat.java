package edu.xdu.psy.gplp.custom;

import java.io.IOException;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * 
 * @author psy
 * 自定义VertexValueOutputFormat
 */

public class GPLPVertexValueOutputFormat extends
TextVertexOutputFormat<LongWritable, VertexValue, EdgeValue> {
/** Specify the output delimiter */
public static final String LINE_TOKENIZE_VALUE = "output.delimiter";
/** Default output delimiter */
public static final String LINE_TOKENIZE_VALUE_DEFAULT = "\t";

@Override
public TextVertexWriter createVertexWriter(TaskAttemptContext context) {
return new SpinnerVertexValueWriter();
}

protected class SpinnerVertexValueWriter extends
	TextVertexWriterToEachLine {
/** Saved delimiter */
private String delimiter;

@Override
public void initialize(TaskAttemptContext context)
		throws IOException, InterruptedException {
	super.initialize(context);
	Configuration conf = context.getConfiguration();
	delimiter = conf.get(LINE_TOKENIZE_VALUE,
			LINE_TOKENIZE_VALUE_DEFAULT);
}

@Override
protected Text convertVertexToLine(
		Vertex<LongWritable, VertexValue, EdgeValue> vertex)
		throws IOException {
	return new Text(vertex.getId().get() + delimiter
			+ vertex.getValue().getCurrentPartition());
}
}
}