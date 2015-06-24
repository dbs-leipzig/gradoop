package org.gradoop.algorithms;

import com.google.common.collect.Maps;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
import org.apache.giraph.io.formats.LongLongNullTextInputFormat;
import org.apache.giraph.utils.InternalVertexRunner;
import org.gradoop.io.formats.LabelPropagationInputFormat;
import org.gradoop.io.formats.LabelPropagationOutputFormat;
import org.junit.Test;

import java.util.Map;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link org.gradoop.algorithms}
 */
public class LabelPropagationComputationTest {
  private static final Pattern LINE_TOKEN_SEPARATOR =
    Pattern.compile(" ");

  @Test
  public void testConnectedGraph() throws Exception {
    String[] graph = GiraphTestHelper.getConnectedGraphWithVertexValues();
    validateConnectedGraphResult(computeResults(graph));
  }

  @Test
  public void testLoopGraph() throws Exception {
    String[] graph = GiraphTestHelper.getLoopGraphWithVertexValues();
    validateLoopGraphResult(computeResults(graph));
  }

  @Test
  public void testDisconnectedGraph() throws Exception {
    String[] graph = GiraphTestHelper.getDisconnectedGraphWithVertexValues();
    validateDisconnectedGraphResult(computeResults(graph));
  }

  @Test
  public void testBipartiteGraph() throws Exception {
    String[] graph = GiraphTestHelper.getBipartiteGraphWithVertexValues();
    validateBipartiteGraphResult(computeResults(graph));
  }

  @Test
  public void testCompleteBipartiteGraph() throws Exception {
    String[] graph =
      GiraphTestHelper.getCompleteBipartiteGraphWithVertexValue();
    validateCompleteBipartiteGraphResult(computeResults(graph));
  }

  private void validateConnectedGraphResult(
    Map<Integer, Integer> vertexIDwithValue) {
    assertEquals(12, vertexIDwithValue.size());
    assertEquals(0, vertexIDwithValue.get(0).longValue());
    assertEquals(0, vertexIDwithValue.get(1).longValue());
    assertEquals(0, vertexIDwithValue.get(2).longValue());
    assertEquals(0, vertexIDwithValue.get(3).longValue());
    assertEquals(4, vertexIDwithValue.get(4).longValue());
    assertEquals(4, vertexIDwithValue.get(5).longValue());
    assertEquals(4, vertexIDwithValue.get(6).longValue());
    assertEquals(4, vertexIDwithValue.get(7).longValue());
    assertEquals(8, vertexIDwithValue.get(8).longValue());
    assertEquals(8, vertexIDwithValue.get(9).longValue());
    assertEquals(8, vertexIDwithValue.get(10).longValue());
    assertEquals(8, vertexIDwithValue.get(11).longValue());
  }

  private void validateLoopGraphResult(
    Map<Integer, Integer> vertexIDwithValue) {
    assertEquals(4, vertexIDwithValue.size());
    assertEquals(0, vertexIDwithValue.get(0).longValue());
    assertEquals(0, vertexIDwithValue.get(1).longValue());
    assertEquals(0, vertexIDwithValue.get(2).longValue());
    assertEquals(0, vertexIDwithValue.get(3).longValue());
  }

  private void validateDisconnectedGraphResult(
    Map<Integer, Integer> vertexIDwithValue) {
    assertEquals(8, vertexIDwithValue.size());
    assertEquals(0, vertexIDwithValue.get(0).longValue());
    assertEquals(0, vertexIDwithValue.get(1).longValue());
    assertEquals(0, vertexIDwithValue.get(2).longValue());
    assertEquals(0, vertexIDwithValue.get(3).longValue());
    assertEquals(4, vertexIDwithValue.get(4).longValue());
    assertEquals(4, vertexIDwithValue.get(5).longValue());
    assertEquals(4, vertexIDwithValue.get(6).longValue());
    assertEquals(4, vertexIDwithValue.get(7).longValue());
  }

  private void validateBipartiteGraphResult(
    Map<Integer, Integer> vertexIDwithValue) {
    assertEquals(8, vertexIDwithValue.size());
    assertEquals(0, vertexIDwithValue.get(0).longValue());
    assertEquals(0, vertexIDwithValue.get(4).longValue());
    assertEquals(1, vertexIDwithValue.get(1).longValue());
    assertEquals(1, vertexIDwithValue.get(5).longValue());
    assertEquals(2, vertexIDwithValue.get(2).longValue());
    assertEquals(2, vertexIDwithValue.get(6).longValue());
    assertEquals(3, vertexIDwithValue.get(3).longValue());
    assertEquals(3, vertexIDwithValue.get(7).longValue());
  }

  private void validateCompleteBipartiteGraphResult(
    Map<Integer, Integer> vertexIDwithValue) {
    assertEquals(8, vertexIDwithValue.size());
    assertEquals(0, vertexIDwithValue.get(0).longValue());
    assertEquals(0, vertexIDwithValue.get(1).longValue());
    assertEquals(0, vertexIDwithValue.get(2).longValue());
    assertEquals(0, vertexIDwithValue.get(3).longValue());
    assertEquals(0, vertexIDwithValue.get(4).longValue());
    assertEquals(0, vertexIDwithValue.get(5).longValue());
    assertEquals(0, vertexIDwithValue.get(6).longValue());
    assertEquals(0, vertexIDwithValue.get(7).longValue());
  }

  private Map<Integer, Integer> computeResults(String[] graph) throws
    Exception {
    GiraphConfiguration conf = getConfiguration();
    Iterable<String> results = InternalVertexRunner.run(conf, graph);
    return parseResults(results);
  }

  private GiraphConfiguration getConfiguration() {
    GiraphConfiguration conf = new GiraphConfiguration();
    conf.setComputationClass(LabelPropagationComputation.class);
    conf.setMasterComputeClass(LabelPropagationMasterComputation.class);
    conf.setVertexInputFormatClass(LabelPropagationInputFormat.class);
    conf.setVertexOutputFormatClass(LabelPropagationOutputFormat.class);
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
