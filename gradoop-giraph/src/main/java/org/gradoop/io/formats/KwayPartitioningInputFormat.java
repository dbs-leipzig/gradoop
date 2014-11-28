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
 * Created by gomezk on 28.11.14.
 */
public class KwayPartitioningInputFormat extends
  TextVertexInputFormat<IntWritable, KwayPartitioningVertex, NullWritable> {

  /**
   * Separator of the vertex and neighbors
   */
  private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

  @Override
  public TextVertexReader createVertexReader(InputSplit split,
                                             TaskAttemptContext context)
    throws IOException {
    return new TwoValueVertexReader();
  }

  public class TwoValueVertexReader extends
    TextVertexReaderFromEachLineProcessed<String[]> {
    /**
     * Cached vertex id for the current line
     */
    private int id;
    /**
     * Cached vertex last value for the current line
     */
    private int lastvalue;
    /**
     * Cached vertex current value for the current line
     */
    private int currentvalue;

    @Override
    protected String[] preprocessLine(Text line)
      throws IOException {
      String[] tokens = SEPARATOR.split(line.toString());
      id = Integer.parseInt(tokens[0]);
      lastvalue = Integer.parseInt(tokens[1]);
      currentvalue = Integer.parseInt(tokens[2]);
      return tokens;
    }

    @Override
    protected IntWritable getId(String[] tokens)
      throws IOException {
      return new IntWritable(id);
    }

    @Override
    protected KwayPartitioningVertex getValue(String[] tokens)
      throws IOException {
      KwayPartitioningVertex vertex = new KwayPartitioningVertex();
      vertex.setCurrentVertexValue(new IntWritable(currentvalue));
      vertex.setLastVertexValue(new IntWritable(lastvalue));
      return vertex;
    }


    @Override
    protected Iterable<Edge<IntWritable, NullWritable>> getEdges(
      String[] tokens)
      throws IOException {
      List<Edge<IntWritable, NullWritable>> edges =
        Lists.newArrayListWithCapacity(tokens.length - 3);
      for (int n = 3; n < tokens.length; n++) {
        edges.add(EdgeFactory.create(
          new IntWritable(Integer.parseInt(tokens[n]))));
      }
      return edges;
    }
  }

}
