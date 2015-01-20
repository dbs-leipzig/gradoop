package org.gradoop.algorithms;

import com.google.common.collect.Maps;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
import org.apache.giraph.utils.InternalVertexRunner;
import org.gradoop.io.formats.AdaptiveRepartitioningInputFormat;
import org.gradoop.io.formats.AdaptiveRepartitioningOutputFormat;
import org.junit.Test;

import java.util.Map;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link AdaptiveRepartitioningComputation}
 */
public class AdaptiveRepartitioningComputationTest {

  private static final Pattern LINE_TOKEN_SEPARATOR =
    Pattern.compile(IdWithValueTextOutputFormat.LINE_TOKENIZE_VALUE_DEFAULT);

  @Test
  public void testSmallConnectedGraph() throws Exception {
    String[] graph =
      PartitioningComputationTestHelper.getKwaySmallConnectedGraph();
    validateSmallConnectedGraphResult(computeResults(graph, 2, 120, 0.5f, 1));
  }

//  @Test
//  public void testBiPartiteGraph() throws Exception {
//    String[] graph = PartitioningComputationTestHelper
// .getKwayBiPartiteGraph();
//    validateBiPartiteGraphResult(computeResults(graph, 2));
//  }


  private void validateSmallConnectedGraphResult(
    Map<Integer, Integer> vertexIDwithValue) {

    assertEquals(8, vertexIDwithValue.size());
    assertEquals(0, vertexIDwithValue.get(0).intValue());
    assertEquals(1, vertexIDwithValue.get(1).intValue());
    assertEquals(0, vertexIDwithValue.get(2).intValue());
    assertEquals(1, vertexIDwithValue.get(3).intValue());
    assertEquals(0, vertexIDwithValue.get(4).intValue());
    assertEquals(1, vertexIDwithValue.get(5).intValue());
    assertEquals(0, vertexIDwithValue.get(6).intValue());
    assertEquals(1, vertexIDwithValue.get(7).intValue());

  }

  private void validateBiPartiteGraphResult(
    Map<Integer, Integer> vertexIDwithValue) {
    assertEquals(10, vertexIDwithValue.size());
    assertEquals(0, vertexIDwithValue.get(0).intValue());
    assertEquals(1, vertexIDwithValue.get(1).intValue());
    assertEquals(0, vertexIDwithValue.get(2).intValue());
    assertEquals(1, vertexIDwithValue.get(3).intValue());
    assertEquals(0, vertexIDwithValue.get(4).intValue());
    assertEquals(1, vertexIDwithValue.get(5).intValue());
    assertEquals(0, vertexIDwithValue.get(6).intValue());
    assertEquals(1, vertexIDwithValue.get(7).intValue());
    assertEquals(0, vertexIDwithValue.get(8).intValue());
    assertEquals(1, vertexIDwithValue.get(9).intValue());
  }


  private Map<Integer, Integer> computeResults(String[] graph,
    int partitionCount, int maxIterations, float capacityTreshold, int
    maxStabilization)
    throws
    Exception {
    GiraphConfiguration conf = getConfiguration();
    conf.setInt(AdaptiveRepartitioningComputation.NUMBER_OF_PARTITIONS,
      partitionCount);
    conf.setInt(AdaptiveRepartitioningComputation.NUMBER_OF_ITERATIONS,
      maxIterations);
    conf.setFloat(AdaptiveRepartitioningComputation.CAPACITY_THRESHOLD,
      capacityTreshold);
    conf.setInt(AdaptiveRepartitioningComputation
      .NUMBER_OF_STABILIZATION_ROUNDS, maxStabilization);
    conf.setBoolean(AdaptiveRepartitioningInputFormat.PARTITIONED_INPUT, true);
    Iterable<String> results = InternalVertexRunner.run(conf, graph);

    return parseResults(results);
  }

  private GiraphConfiguration getConfiguration() {
    GiraphConfiguration conf = new GiraphConfiguration();
    conf.setComputationClass(AdaptiveRepartitioningComputation.class);
    conf.setMasterComputeClass(AdaptiveRepartitioningMasterComputation.class);
    conf.setVertexInputFormatClass(AdaptiveRepartitioningInputFormat.class);
    conf.setVertexOutputFormatClass(AdaptiveRepartitioningOutputFormat.class);
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
