package org.gradoop.io.formats;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.gradoop.io.KwayPartitioningVertex;

import java.io.IOException;

/**
 * Encodes the output of the {@link org.gradoop.algorithms.KwayPartitioningComputation} in
 * the following format: vertex-id vertex-value
 */
public class KwayPartitioningOutputFormat extends
  TextVertexOutputFormat<IntWritable, KwayPartitioningVertex, NullWritable> {

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
  public TextVertexWriter createVertexWriter(TaskAttemptContext context)
    throws IOException, InterruptedException {
    return new KwayTextVertexLineWriter();
  }

  /**
   *  Used to convert a {@link org.gradoop.io.KwayPartitioningVertex} to a line in the output file.
   */
  private class KwayTextVertexLineWriter extends TextVertexWriterToEachLine {

    /**
     * Writes a line for the given vertex.
     *
     * @param vertex the current vertex for writing
     * @return the text line to be written
     * @throws java.io.IOException exception that can be thrown while writing
     */
    @Override
    protected Text convertVertexToLine(
      Vertex<IntWritable, KwayPartitioningVertex, NullWritable> vertex)
      throws IOException {
      StringBuilder sb = new StringBuilder();
      // vertex-id
      sb.append(vertex.getId());
      sb.append(VALUE_TOKEN_SEPARATOR);
      sb.append(vertex.getValue().getCurrentVertexValue());
      return new Text(sb.toString());
    }
  }
}
