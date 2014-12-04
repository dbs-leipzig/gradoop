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

  private static final Pattern LINE_TOKEN_SEPARATOR = Pattern.compile(" ");

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
      "0 0 0 1 2",
      "1 0 0 0 2",
      "2 0 0 0 1 3",
      "3 0 0 2 4 5",
      "4 0 0 3 5",
      "5 0 0 3 4"
    };
  }



  private void validateSmallConnectedGraphResult(
    Map<Integer, Integer> vertexIDwithValue) {
    assertEquals(6, vertexIDwithValue.size());
    if (0 == vertexIDwithValue.get(2)) {
      if (1 == vertexIDwithValue.get(3)) {
        assertEquals(0, vertexIDwithValue.get(0).intValue());
        assertEquals(0, vertexIDwithValue.get(1).intValue());
        assertEquals(0, vertexIDwithValue.get(2).intValue());
        assertEquals(1, vertexIDwithValue.get(3).intValue());
        assertEquals(1, vertexIDwithValue.get(4).intValue());
        assertEquals(1, vertexIDwithValue.get(5).intValue());

      }
    }

    if (1 == vertexIDwithValue.get(2)) {
      if (0 == vertexIDwithValue.get(3)) {
        assertEquals(1, vertexIDwithValue.get(0).intValue());
        assertEquals(1, vertexIDwithValue.get(1).intValue());
        assertEquals(1, vertexIDwithValue.get(2).intValue());
        assertEquals(0, vertexIDwithValue.get(3).intValue());
        assertEquals(0, vertexIDwithValue.get(4).intValue());
        assertEquals(0, vertexIDwithValue.get(5).intValue());

      }
    }

    if (1 == vertexIDwithValue.get(2)) {
      if (1 == vertexIDwithValue.get(3)) {
        assertEquals(1, vertexIDwithValue.get(0).intValue());
        assertEquals(1, vertexIDwithValue.get(1).intValue());
        assertEquals(1, vertexIDwithValue.get(2).intValue());
        assertEquals(1, vertexIDwithValue.get(3).intValue());
        assertEquals(1, vertexIDwithValue.get(4).intValue());
        assertEquals(1, vertexIDwithValue.get(5).intValue());

      }
    }

    if (0 == vertexIDwithValue.get(2)) {
      if (0 == vertexIDwithValue.get(3)) {
        assertEquals(0, vertexIDwithValue.get(0).intValue());
        assertEquals(0, vertexIDwithValue.get(1).intValue());
        assertEquals(0, vertexIDwithValue.get(2).intValue());
        assertEquals(0, vertexIDwithValue.get(3).intValue());
        assertEquals(0, vertexIDwithValue.get(4).intValue());
        assertEquals(0, vertexIDwithValue.get(5).intValue());

      }
    } else {
      assertTrue("Something else", false);
    }


  }


  private Map<Integer, Integer> computeResults(String[] graph)
    throws Exception {
    GiraphConfiguration conf = getConfiguration();
    Iterable<String> results = InternalVertexRunner.run(conf, graph);
    return parseResults(results);
  }

  private GiraphConfiguration getConfiguration() {
    GiraphConfiguration conf = new GiraphConfiguration();
    conf.setComputationClass(KwayPartitioningComputation.class);
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
