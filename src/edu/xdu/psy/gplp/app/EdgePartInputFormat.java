package edu.xdu.psy.gplp.app;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class EdgePartInputFormat extends TextEdgeInputFormat<IdValue, FloatWritable> {
	private static final Pattern SEPARATOR = Pattern.compile("[\001\t ]");
	public static final String DELIMITER = "_";

	@Override
	public EdgeReader<IdValue, FloatWritable> createEdgeReader(
			InputSplit split, TaskAttemptContext context) throws IOException {
		return new SPREdgeReader();
	}

	public class SPREdgeReader extends
			TextEdgeReaderFromEachLineProcessed<String[]> {
		@Override
		protected String[] preprocessLine(Text line) throws IOException {
			return SEPARATOR.split(line.toString());
		}

		@Override
		protected IdValue getSourceVertexId(String[] endpoints)
				throws IOException {
			return new IdValue(endpoints[0]);
		}

		@Override
		protected IdValue getTargetVertexId(String[] endpoints)
				throws IOException {
			return new IdValue(endpoints[1]);
		}

		@Override
		protected FloatWritable getValue(String[] endpoints) throws IOException {
			return new FloatWritable(Float.parseFloat(endpoints[2]));
		}
	}

}