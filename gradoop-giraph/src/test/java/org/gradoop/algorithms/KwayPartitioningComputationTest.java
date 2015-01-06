package org.gradoop.algorithms;

import com.google.common.collect.Maps;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.utils.InternalVertexRunner;
import org.gradoop.io.formats.KwayPartitioningInputFormat;
import org.gradoop.io.formats.KwayPartitioningOutputFormat;
import org.junit.Test;

import java.util.Map;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link org.gradoop.algorithms.KwayPartitioningComputation}
 */
public class KwayPartitioningComputationTest {

  private static final Pattern LINE_TOKEN_SEPARATOR = Pattern.compile("\t");

  @Test
  public void testSmallConnectedGraph() throws Exception {
    String[] graph =
      PartitioningComputationTestHelper.getKwaySmallConnectedGraph();
    validateSmallConnectedGraphResult(computeResults(graph, 2, "rdm"));
  }


  private void validateSmallConnectedGraphResult(
    Map<Integer, Integer> vertexIDwithValue) {


    if (0 == vertexIDwithValue.get(0)) {
      if (1 == vertexIDwithValue.get(4)) {
        assertEquals(8, vertexIDwithValue.size());
        assertEquals(0, vertexIDwithValue.get(0).intValue());
        assertEquals(0, vertexIDwithValue.get(1).intValue());
        assertEquals(0, vertexIDwithValue.get(2).intValue());
        assertEquals(0, vertexIDwithValue.get(3).intValue());
        assertEquals(1, vertexIDwithValue.get(4).intValue());
        assertEquals(1, vertexIDwithValue.get(5).intValue());
        assertEquals(1, vertexIDwithValue.get(6).intValue());
        assertEquals(1, vertexIDwithValue.get(7).intValue());
      }
    }

    if(1 == vertexIDwithValue.get(0)){
      if(0 == vertexIDwithValue.get(4)){
        assertEquals(8, vertexIDwithValue.size());
        assertEquals(1, vertexIDwithValue.get(0).intValue());
        assertEquals(1, vertexIDwithValue.get(1).intValue());
        assertEquals(1, vertexIDwithValue.get(2).intValue());
        assertEquals(1, vertexIDwithValue.get(4).intValue());
        assertEquals(0, vertexIDwithValue.get(5).intValue());
        assertEquals(0, vertexIDwithValue.get(6).intValue());
        assertEquals(0, vertexIDwithValue.get(7).intValue());
        assertEquals(0, vertexIDwithValue.get(8).intValue());
      }
    }
  }


  private Map<Integer, Integer> computeResults(String[] graph,
    int partitionCount, String computation_case) throws Exception {
    GiraphConfiguration conf = getConfiguration();
    conf.set(KwayPartitioningComputation.NUMBER_OF_PARTITIONS,
      Integer.toString(partitionCount));
    conf.set(KwayPartitioningComputation.COMPUTATION_CASE, computation_case);
    Iterable<String> results = InternalVertexRunner.run(conf, graph);
    return parseResults(results);
  }

  private GiraphConfiguration getConfiguration() {
    GiraphConfiguration conf = new GiraphConfiguration();
    conf.setComputationClass(KwayPartitioningComputation.class);
    conf.setMasterComputeClass(KwayPartitioningMasterComputation.class);
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
}
