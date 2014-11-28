package org.gradoop.io.formats;

import com.google.common.collect.Maps;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.gradoop.io.KwayPartitioningVertex;
import org.junit.Test;

import java.util.Map;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;

/**
 * Created by gomezk on 28.11.14.
 */
public class KwayPartitioningInputFormatTest {

  private static final Pattern LINE_TOKEN_SEPARATOR = Pattern.compile
    (IdWithValueTextOutputFormat.LINE_TOKENIZE_VALUE_DEFAULT);

  @Test
  public void testSmallConnectedGraph()
    throws Exception {
    String[] graph = getSmallConnectedGraph();
    validateSmallConnectedGraphResult(computeResults(graph));
  }

  /**
   * @return a small graph with two connected partitions
   */
  protected String[] getSmallConnectedGraph() {
    return new String[]{
      "0 7 0 1 2 3",
      "1 6 1 0 2 3",
      "2 5 2 0 1 3 4",
      "3 4 3 0 1 2",
      "4 3 4 2 5 6 7",
      "5 2 5 4 6 7",
      "6 1 6 4 5 7",
      "7 0 7 4 5 6"
    };
  }

  private void validateSmallConnectedGraphResult(
    Map<Integer, Integer> vertexIDwithValue) {
    assertEquals(3, vertexIDwithValue.get(0).intValue());
    assertEquals(3, vertexIDwithValue.get(1).intValue());
    assertEquals(4, vertexIDwithValue.get(2).intValue());
    assertEquals(3, vertexIDwithValue.get(3).intValue());
    assertEquals(7, vertexIDwithValue.get(4).intValue());
    assertEquals(7, vertexIDwithValue.get(5).intValue());
    assertEquals(7, vertexIDwithValue.get(6).intValue());
    assertEquals(7, vertexIDwithValue.get(7).intValue());

  }

  private Map<Integer, Integer> computeResults(String[] graph)
    throws Exception {
    GiraphConfiguration conf = getConfiguration();
    Iterable<String> results = InternalVertexRunner.run(conf, graph);
    return parseResults(results);
  }

  private GiraphConfiguration getConfiguration() {
    GiraphConfiguration conf = new GiraphConfiguration();
    conf.setComputationClass(GetLastValueComputation.class);
    conf.setVertexInputFormatClass(KwayPartitioningInputFormat.class);
    conf.setVertexOutputFormatClass(IdWithValueTextOutputFormat.class);
    return conf;
  }

  private Map<Integer, Integer> parseResults(Iterable<String> results) {
    Map<Integer, Integer> parsedResults = Maps.newHashMap();
    String[] lineTokens;
    int value;
    int vertexID;
    for (String line : results) {
      lineTokens = LINE_TOKEN_SEPARATOR.split(line);
      vertexID = Integer.parseInt(lineTokens[0]);
      value = Integer.parseInt(lineTokens[1]);
      parsedResults.put(vertexID, value);
    }
    return parsedResults;
  }

  /**
   * Example Computation that get the LastValue and save it as CurrentValue
   */
  public static class GetLastValueComputation extends
    BasicComputation<IntWritable, KwayPartitioningVertex, NullWritable, IntWritable> {

    @Override
    public void compute(
      Vertex<IntWritable, KwayPartitioningVertex, NullWritable> vertex,
      Iterable<IntWritable> messages) {
      if (getSuperstep() == 0) {
        vertex.getValue().setLastVertexValue(vertex.getId());
        sendMessageToAllEdges(vertex, vertex.getValue().getLastVertexValue());
      } else {
        int maxValue = vertex.getValue().getCurrentVertexValue().get();
        for (IntWritable message : messages) {
          int messageValue = message.get();
          if (messageValue > maxValue) {
            maxValue = messageValue;
          }
        }
        vertex.getValue().setCurrentVertexValue(new IntWritable(maxValue));
      }
      vertex.voteToHalt();
    }
  }
}
