/**
 * 
 * 
 * 毕设
 * SLGP-α
 * 
 * 
 */
package edu.xdu.psy.gplp.app;

import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;

/**
 * Basic Pregel PageRank implementation.
 *
 * This version initializes the value of every vertex to 1/N, where N is the
 * total number of vertices.
 *
 * The maximum number of supersteps is configurable.
 */
public class D_PageRank extends BasicComputation<LongWritable,
  DoubleWritable, FloatWritable, DoubleWritable> {
  /** Default number of supersteps */
  public static final int MAX_SUPERSTEPS_DEFAULT = 90;
  /** Property name for number of supersteps */
  public static final String MAX_SUPERSTEPS = "pagerank.max.supersteps";

  /** Logger */
  private static final Logger LOG =
    Logger.getLogger(D_PageRank.class);

  @Override
  public void compute(
      Vertex<LongWritable, DoubleWritable, FloatWritable> vertex,
      Iterable<DoubleWritable> messages) {
    if (getSuperstep() == 0) {
      vertex.setValue(new DoubleWritable(1f / getTotalNumVertices()));
    }
    if (getSuperstep() >= 1) {
      double sum = 0;
      for (DoubleWritable message : messages) {
        sum += message.get();
      }
      DoubleWritable vertexValue =
        new DoubleWritable((0.15f / getTotalNumVertices()) + 0.85f * sum);
      vertex.setValue(vertexValue);
    }

    if (getSuperstep() < getContext().getConfiguration().getInt(
      MAX_SUPERSTEPS, MAX_SUPERSTEPS_DEFAULT)) {
      long edges = vertex.getNumEdges();
      sendMessageToAllEdges(vertex,
          new DoubleWritable(vertex.getValue().get() / edges));
    } else {
      vertex.voteToHalt();
    }
  }
  
  
  

}
