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
public class KwayPartitioningVertexFormatTest {

  private static final Pattern LINE_TOKEN_SEPARATOR = Pattern.compile
    (" ");

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
      "0 7 0 1",
      "1 6 0 0",
      "2 5 0 3",
      "3 4 0 2",
      "4 3 0 5",
      "5 2 0 4",
      "6 1 0 7",
      "7 0 0 6"
    };
  }

  private void validateSmallConnectedGraphResult(
    Map<Integer, Integer> vertexIDwithValue) {
    assertEquals(7, vertexIDwithValue.get(0).intValue());
    assertEquals(7, vertexIDwithValue.get(1).intValue());
    assertEquals(5, vertexIDwithValue.get(2).intValue());
    assertEquals(5, vertexIDwithValue.get(3).intValue());
    assertEquals(3, vertexIDwithValue.get(4).intValue());
    assertEquals(3, vertexIDwithValue.get(5).intValue());
    assertEquals(1, vertexIDwithValue.get(6).intValue());
    assertEquals(1, vertexIDwithValue.get(7).intValue());

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
    conf.setVertexOutputFormatClass(KwayPartitioningOutputFormat.class);
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
        sendMessageToAllEdges(vertex, vertex.getValue().getLastVertexValue());
        vertex.voteToHalt();
      }else{
        int currentValue = vertex.getValue().getCurrentVertexValue().get();

        for(IntWritable lastValue: messages){
          if(currentValue<lastValue.get()){
            vertex.getValue().setCurrentVertexValue(lastValue);
            sendMessageToAllEdges(vertex,vertex.getValue().getCurrentVertexValue
              ());
            vertex.voteToHalt();
          }
        }
      }
      vertex.voteToHalt();
    }
  }
}
