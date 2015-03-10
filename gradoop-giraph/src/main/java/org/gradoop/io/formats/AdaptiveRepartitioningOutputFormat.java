package org.gradoop.io.formats;

import org.apache.commons.lang3.StringUtils;
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
 * {@code <vertex-id> <partition-id> \[<partition-id>*\] [<neighbour-id>]*}
 */
public class AdaptiveRepartitioningOutputFormat extends
  TextVertexOutputFormat<IntWritable, PartitioningVertex, NullWritable> {
  /**
   * Used to tell the output format if the partition history should be
   * printed or not
   */
  public static final String PARTITION_HISTORY_OUTPUT =
    "partitioning.output" + ".partitionhistory";
  /**
   * Default value for PARTITION_HISTORY_OUTPUT.
   */
  public static final boolean DEFAULT_PARTITION_HISTORY_OUTPUT = false;
  /**
   * Used for splitting the line into the main tokens (vertex id, vertex value
   */
  private static final String VALUE_TOKEN_SEPARATOR = " ";
  /**
   * Starts partition history block
   */
  private static final String LIST_BLOCK_OPEN = "[";
  /**
   * Closes partition history block
   */
  private static final String LIST_BLOCK_CLOSE = "]";
  /**
   * Used to separate partition ids in partition history block.
   */
  private static final String PARTITION_HISTORY_SEPARATOR = ",";
  /**
   * Used to decide if Partition History should be printed or not.
   */
  private boolean historyOutput;

  /**
   * @param context the information about the task
   * @return the text vertex writer to be used
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  public TextVertexWriter createVertexWriter(TaskAttemptContext context) throws
    IOException, InterruptedException {
    this.historyOutput = getConf()
      .getBoolean(PARTITION_HISTORY_OUTPUT, DEFAULT_PARTITION_HISTORY_OUTPUT);
    return new AdaptiveRepartitioningTextVertexLineWriter();
  }

  /**
   * Used to convert a {@link org.gradoop.io.PartitioningVertex} to a
   * line in the output file.
   */
  private class AdaptiveRepartitioningTextVertexLineWriter extends
    TextVertexWriterToEachLine {
    /**
     * {@inheritDoc}
     */
    @Override
    protected Text convertVertexToLine(
      Vertex<IntWritable, PartitioningVertex, NullWritable> vertex) throws
      IOException {
      // vertex id
      StringBuilder sb = new StringBuilder(vertex.getId().toString());
      sb.append(VALUE_TOKEN_SEPARATOR);
      // vertex value
      sb.append(vertex.getValue().getCurrentPartition());
      sb.append(VALUE_TOKEN_SEPARATOR);
      // vertex partition history
      if (historyOutput) {
        sb.append(LIST_BLOCK_OPEN);
        if (vertex.getValue().getPartitionHistoryCount() > 0) {
          sb.append(StringUtils.join(vertex.getValue().getPartitionHistory(),
            PARTITION_HISTORY_SEPARATOR));
        }
        sb.append(LIST_BLOCK_CLOSE);
        sb.append(VALUE_TOKEN_SEPARATOR);
      }
      // edges
      for (Edge<IntWritable, NullWritable> e : vertex.getEdges()) {
        sb.append(e.getTargetVertexId());
        sb.append(VALUE_TOKEN_SEPARATOR);
      }
      return new Text(sb.toString());
    }
  }
}
