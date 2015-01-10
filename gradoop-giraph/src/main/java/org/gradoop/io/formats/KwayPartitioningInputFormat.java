package org.gradoop.io.formats;

import com.google.common.collect.Lists;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.gradoop.io.KwayPartitioningVertex;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

/**
 * An KwayPartitioning vertex is decoded in the following format: vertex-id
 * vertex-last_value vertex-current_value [neighbour-vertex-id ]* e.g. the
 * following line: 5 3 4 4 6 7 decodes vertex-id 5 last_value 3 and actual_value
 * 4. The node is connected to three other nodes (4 6 7)
 */
public class KwayPartitioningInputFormat extends
  TextVertexInputFormat<IntWritable, KwayPartitioningVertex, NullWritable> {

  /**
   * Separator of the vertex and neighbors
   */
  private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

  /**
   * @param split   the split to be read
   * @param context the information about the task
   * @return the text vertex reader to be used
   * @throws IOException
   */
  @Override
  public TextVertexReader createVertexReader(InputSplit split,
    TaskAttemptContext context) throws IOException {
    return new TwoValueVertexReader();
  }

  /**
   * Reads a vertex with two values from an input line.
   */
  public class TwoValueVertexReader extends
    TextVertexReaderFromEachLineProcessed<String[]> {
    /**
     * Cached vertex id for the current line
     */
    private int id;
    /**
     * Cached vertex last_value for the current line
     */
    private int lastValue;
    /**
     * Cached vertex current_value for the current line
     */
    private int currentValue;

    /**
     * Reads every single line and returns the tokens as array
     *
     * @param line line of input
     * @return line tokens as array
     * @throws IOException
     */
    @Override
    protected String[] preprocessLine(Text line) throws IOException {
      String[] tokens = SEPARATOR.split(line.toString());
      id = Integer.parseInt(tokens[0]);
      lastValue = Integer.parseInt(tokens[1]);
      currentValue = Integer.parseInt(tokens[2]);
      return tokens;
    }

    /**
     * Returns the vertex ID
     *
     * @param tokens line tokens
     * @return the vertex ID as Writable
     * @throws IOException
     */
    @Override
    protected IntWritable getId(String[] tokens) throws IOException {
      return new IntWritable(id);
    }

    /**
     * Returns the new KwayPartitioning vertex
     *
     * @param tokens line tokens
     * @return vertex with set params
     * @throws IOException
     */
    @Override
    protected KwayPartitioningVertex getValue(String[] tokens) throws
      IOException {
      KwayPartitioningVertex vertex = new KwayPartitioningVertex();
      vertex.setCurrentVertexValue(new IntWritable(currentValue));
      vertex.setLastVertexValue(new IntWritable(lastValue));
      return vertex;
    }

    /**
     * Returns all edges of the vertex
     *
     * @param tokens line tokens
     * @return edges of the vertex
     * @throws IOException
     */
    @Override
    protected Iterable<Edge<IntWritable, NullWritable>> getEdges(
      String[] tokens) throws IOException {
      List<Edge<IntWritable, NullWritable>> edges =
        Lists.newArrayListWithCapacity(tokens.length - 3);
      for (int n = 3; n < tokens.length; n++) {
        edges.add(
          EdgeFactory.create(new IntWritable(Integer.parseInt(tokens[n]))));
      }
      return edges;
    }
  }
}
