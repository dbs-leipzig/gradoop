package org.gradoop.io.formats;

import com.google.common.collect.Lists;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.gradoop.io.LabelPropagationValue;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

/**
 * A LabelPropagation vertex is decoded in the following format:
 * {@code <vertex-id> [<neighbour-id>]*}
 */
public class LabelPropagationInputFormat extends
  TextVertexInputFormat<LongWritable, LabelPropagationValue, NullWritable> {
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
    return new VertexReader();
  }

  /**
   * Reads a vertex with two values from an input line.
   */
  public class VertexReader extends
    TextVertexReaderFromEachLineProcessed<String[]> {
    /**
     * Cached vertex id for the current line
     */
    private int id;
    /**
     * Cached vertex current Partition
     */
    private long lastCommunity = Long.MAX_VALUE;
    /**
     * Cached vertex desired Partition
     */
    private long currentCommunity = 0;
    /**
     * Cached the vertex stabilization round counter
     */
    private long stabilizationRound = 0;

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
      currentCommunity = id;
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
    protected LongWritable getId(String[] tokens) throws IOException {
      return new LongWritable(id);
    }

    /**
     * Returns the new LabelPropagation vertex
     *
     * @param tokens line tokens
     * @return vertex with set params
     * @throws IOException
     */
    @Override
    protected LabelPropagationValue getValue(String[] tokens) throws
      IOException {
      LabelPropagationValue vertex = new LabelPropagationValue();
      vertex.setLastCommunity(new LongWritable(lastCommunity));
      vertex.setCurrentCommunity(new LongWritable(currentCommunity));
      vertex.setStabilizationRounds(stabilizationRound);
      return vertex;
    }

    /**
     * Returns all edges of the vertexInteger
     *
     * @param tokens line tokens
     * @return edges of the vertex
     * @throws IOException
     */
    @Override
    protected Iterable<Edge<LongWritable, NullWritable>> getEdges(
      String[] tokens) throws IOException {
      List<Edge<LongWritable, NullWritable>> edges = Lists.newArrayList();
      for (int n = 2; n < tokens.length; n++) {
        edges
          .add(EdgeFactory.create(new LongWritable(Long.parseLong(tokens[n]))));
      }
      return edges;
    }
  }
}
