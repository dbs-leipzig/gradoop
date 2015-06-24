package org.gradoop.io.formats;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.gradoop.io.LabelPropagationValue;

import java.io.IOException;

/**
 * Encodes the output of the {@link org.gradoop.algorithms
 * .LabelPropagationComputation} in
 * the following format:
 * <p/>
 * {@code <vertex-id> <community-id> [<neighbour-id>]*}
 */
public class LabelPropagationOutputFormat extends
  TextVertexOutputFormat<LongWritable, LabelPropagationValue, NullWritable> {
  /**
   * Used for splitting the line into the main tokens (vertex id, vertex value
   */
  private static final String VALUE_TOKEN_SEPARATOR = " ";

  /**
   * @param context the information about the task
   * @return the text vertex writer to be used
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  public TextVertexWriter createVertexWriter(TaskAttemptContext context) throws
    IOException, InterruptedException {
    return new LabelPropagationTextVertexLineWriter();
  }

  /**
   * Used to convert a {@link org.gradoop.io.PartitioningVertex} to a
   * line in the output file.
   */
  private class LabelPropagationTextVertexLineWriter extends
    TextVertexWriterToEachLine {
    /**
     * {@inheritDoc}
     */
    @Override
    protected Text convertVertexToLine(
      Vertex<LongWritable, LabelPropagationValue, NullWritable> vertex) throws
      IOException {
      // vertex id
      StringBuilder sb = new StringBuilder(vertex.getId().toString());
      sb.append(VALUE_TOKEN_SEPARATOR);
      // vertex value
      sb.append(vertex.getValue().getCurrentCommunity().get());
      sb.append(VALUE_TOKEN_SEPARATOR);
      // vertex partition history
      // edges
      for (Edge<LongWritable, NullWritable> e : vertex.getEdges()) {
        sb.append(e.getTargetVertexId());
        sb.append(VALUE_TOKEN_SEPARATOR);
      }
      return new Text(sb.toString());
    }
  }
}
