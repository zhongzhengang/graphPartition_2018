package edu.xdu.psy.gplp.custom;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * 
 * @author psy
 * 自定义EdgeInputFormat
 *
 */


public  class GPLPEdgeInputFormat extends
TextEdgeInputFormat<LongWritable, EdgeValue> {
/** Splitter for endpoints */
private static final Pattern SEPARATOR = Pattern.compile("[\001\t ]");

@Override
public EdgeReader<LongWritable, EdgeValue> createEdgeReader(
	InputSplit split, TaskAttemptContext context)
	throws IOException {
return new SpinnerEdgeReader();
}

public class SpinnerEdgeReader extends
	TextEdgeReaderFromEachLineProcessed<String[]> {
@Override
protected String[] preprocessLine(Text line) throws IOException {
	return SEPARATOR.split(line.toString());
}

@Override
protected LongWritable getSourceVertexId(String[] endpoints)
		throws IOException {
	return new LongWritable(Long.parseLong(endpoints[0]));
}

@Override
protected LongWritable getTargetVertexId(String[] endpoints)
		throws IOException {
	return new LongWritable(Long.parseLong(endpoints[1]));
}

@Override
protected EdgeValue getValue(String[] endpoints) throws IOException {
	EdgeValue value = new EdgeValue();
	if (endpoints.length == 3) {
		value.setWeight(Byte.parseByte(endpoints[2]));
	}
	return value;
}
}
}
