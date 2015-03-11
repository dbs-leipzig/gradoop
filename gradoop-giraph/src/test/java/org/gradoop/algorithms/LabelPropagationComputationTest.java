package org.gradoop.algorithms;

import com.google.common.collect.Maps;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
import org.apache.giraph.io.formats.LongLongNullTextInputFormat;
import org.apache.giraph.utils.InternalVertexRunner;
import org.junit.Test;

import java.util.Map;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link org.gradoop.algorithms}
 */
public class LabelPropagationComputationTest {
  private static final Pattern LINE_TOKEN_SEPARATOR = Pattern.compile
    (IdWithValueTextOutputFormat.LINE_TOKENIZE_VALUE_DEFAULT);

  @Test
  public void testConnectedGraph()
    throws Exception {
    String[] graph = GiraphTestHelper.getConnectedGraphWithVertexValues();
    validateConnectedGraphResult(computeResults(graph));
  }

  @Test
  public void testLoopGraph()
    throws Exception {
    String[] graph = GiraphTestHelper.getLoopGraphWithVertexValues();
    validateLoopGraphResult(computeResults(graph));
  }


  @Test
  public void testDisconnectedGraph()
    throws Exception {
    String[] graph = GiraphTestHelper.getDisconnectedGraphWithVertexValues();
    validateDisconnectedGraphResult(computeResults(graph));
  }

  @Test
  public void testBipartiteGraph()
    throws Exception {
    String[] graph = GiraphTestHelper.getBipartiteGraphWithVertexValues();
    validateBipartiteGraphResult(computeResults(graph));
  }

  @Test
  public void testCompleteBipartiteGraph()
    throws Exception {
    String[] graph = GiraphTestHelper.getCompleteBipartiteGraphWithVertexValue();
    validateCompleteBipartiteGraphResult(computeResults(graph));
  }


  private void validateConnectedGraphResult(
    Map<Integer, Integer> vertexIDwithValue) {
    assertEquals(12, vertexIDwithValue.size());
    assertEquals(0, vertexIDwithValue.get(0).intValue());
    assertEquals(0, vertexIDwithValue.get(1).intValue());
    assertEquals(0, vertexIDwithValue.get(2).intValue());
    assertEquals(0, vertexIDwithValue.get(3).intValue());
    assertEquals(4, vertexIDwithValue.get(4).intValue());
    assertEquals(4, vertexIDwithValue.get(5).intValue());
    assertEquals(4, vertexIDwithValue.get(6).intValue());
    assertEquals(4, vertexIDwithValue.get(7).intValue());
    assertEquals(8, vertexIDwithValue.get(8).intValue());
    assertEquals(8, vertexIDwithValue.get(9).intValue());
    assertEquals(8, vertexIDwithValue.get(10).intValue());
    assertEquals(8, vertexIDwithValue.get(11).intValue());
  }

  private void validateLoopGraphResult(
    Map<Integer, Integer> vertexIDwithValue) {
    assertEquals(1, vertexIDwithValue.size());
    assertEquals(0, vertexIDwithValue.get(0).intValue());
  }

  private void validateDisconnectedGraphResult(
    Map<Integer, Integer> vertexIDwithValue) {
    assertEquals(8, vertexIDwithValue.size());
    assertEquals(0, vertexIDwithValue.get(0).intValue());
    assertEquals(0, vertexIDwithValue.get(1).intValue());
    assertEquals(0, vertexIDwithValue.get(2).intValue());
    assertEquals(0, vertexIDwithValue.get(3).intValue());
    assertEquals(4, vertexIDwithValue.get(4).intValue());
    assertEquals(4, vertexIDwithValue.get(5).intValue());
    assertEquals(4, vertexIDwithValue.get(6).intValue());
    assertEquals(4, vertexIDwithValue.get(7).intValue());
  }

  private void validateBipartiteGraphResult(
    Map<Integer, Integer> vertexIDwithValue) {
    assertEquals(8, vertexIDwithValue.size());
    assertEquals(0, vertexIDwithValue.get(0).intValue());
    assertEquals(0, vertexIDwithValue.get(4).intValue());
    assertEquals(1, vertexIDwithValue.get(1).intValue());
    assertEquals(1, vertexIDwithValue.get(5).intValue());
    assertEquals(2, vertexIDwithValue.get(2).intValue());
    assertEquals(2, vertexIDwithValue.get(6).intValue());
    assertEquals(3, vertexIDwithValue.get(3).intValue());
    assertEquals(3, vertexIDwithValue.get(7).intValue());
  }

  private void validateCompleteBipartiteGraphResult(Map<Integer,
    Integer> vertexIDwithValue) {
    assertEquals(8, vertexIDwithValue.size());
    assertEquals(0, vertexIDwithValue.get(0).intValue());
    assertEquals(0, vertexIDwithValue.get(1).intValue());
    assertEquals(0, vertexIDwithValue.get(2).intValue());
    assertEquals(0, vertexIDwithValue.get(3).intValue());
    assertEquals(0, vertexIDwithValue.get(4).intValue());
    assertEquals(0, vertexIDwithValue.get(5).intValue());
    assertEquals(0, vertexIDwithValue.get(6).intValue());
    assertEquals(0, vertexIDwithValue.get(7).intValue());
  }

  private Map<Integer, Integer> computeResults(String[] graph)
    throws Exception {
    GiraphConfiguration conf = getConfiguration();
    Iterable<String> results = InternalVertexRunner.run(conf, graph);
    return parseResults(results);
  }

  private GiraphConfiguration getConfiguration() {
    GiraphConfiguration conf = new GiraphConfiguration();
    conf.setComputationClass(LabelPropagationComputation.class);
    conf.setVertexInputFormatClass(LongLongNullTextInputFormat.class);
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
}
