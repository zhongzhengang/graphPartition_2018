package edu.xdu.psy.gplp.app;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class OrginEdgeInputFormat extends TextEdgeInputFormat<LongWritable, FloatWritable> {
	private static final Pattern SEPARATOR = Pattern.compile("[\001\t ]");

	@Override
	public EdgeReader<LongWritable, FloatWritable> createEdgeReader(
			InputSplit split, TaskAttemptContext context) throws IOException {
		return new OrginEdgeReader();
	}

	public class OrginEdgeReader extends
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
		protected FloatWritable getValue(String[] endpoints) throws IOException {
			return new FloatWritable(Float.parseFloat(endpoints[2]));
		}
	}

}