package org.gradoop.io.formats;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.gradoop.io.PartitioningVertex;

import java.io.IOException;

/**
 * Encodes the output of the {@link org.gradoop.algorithms
 * .AdaptiveRepartitioningComputation} in
 * the following format:
 * <p/>
 * {@code <vertex-id> <vertex-last-value> <vertex-current-value>
 * [<neighbour-id>]*}
 */
public class AdaptiveRepartitioningOutputFormat extends
  TextVertexOutputFormat<IntWritable, PartitioningVertex, NullWritable> {
  /**
   * Used for splitting the line into the main tokens (vertex id, vertex value
   */
  private static final String VALUE_TOKEN_SEPARATOR = "\t";
  private static final String LIST_BLOCK_OPEN = "[";
  private static final String LIST_BLOCK_CLOSE = "]";

  /**
   * @param context the information about the task
   * @return the text vertex writer to be used
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  public TextVertexWriter createVertexWriter(TaskAttemptContext context) throws
    IOException, InterruptedException {
    return new AdaptiveRepartitioningTextVertexLineWriter();
  }

  /**
   * Used to convert a {@link org.gradoop.io.PartitioningVertex} to a
   * line in the output file.
   */
  private class AdaptiveRepartitioningTextVertexLineWriter extends
    TextVertexWriterToEachLine {
    /**
     * Writes a line for the given vertex.
     *
     * @param vertex the current vertex for writing
     * @return the text line to be written
     * @throws java.io.IOException exception that can be thrown while writing
     */
    @Override
    protected Text convertVertexToLine(
      Vertex<IntWritable, PartitioningVertex, NullWritable> vertex) throws
      IOException {
      StringBuilder sb = new StringBuilder(vertex.getId().toString());
      sb.append(VALUE_TOKEN_SEPARATOR);
      sb.append(vertex.getValue().getCurrentPartition());
      sb.append(VALUE_TOKEN_SEPARATOR);
      if (vertex.getValue().getPartitionHistoryCount() != 0) {
        sb.append(vertex.getValue().getPartitionHistory().toString());
      } else {
        sb.append(LIST_BLOCK_OPEN);
        sb.append(LIST_BLOCK_CLOSE);
        sb.append(VALUE_TOKEN_SEPARATOR);
      }
      for (Edge<IntWritable, NullWritable> e : vertex.getEdges()) {
        sb.append(VALUE_TOKEN_SEPARATOR).append(e.getTargetVertexId());
      }
      return new Text(sb.toString());
    }
  }
}
