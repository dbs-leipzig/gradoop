package org.gradoop.algorithms;

import com.google.common.collect.Maps;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
import org.apache.giraph.utils.InternalVertexRunner;
import org.gradoop.io.formats.KwayPartitioningInputFormat;
import org.junit.Test;

import java.util.Map;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;

/**
 * Created by galpha on 27.11.14.
 */
public class KwayPartitioningComputationTest {

  private static final Pattern LINE_TOKEN_SEPARATOR = Pattern.compile
    (IdWithValueTextOutputFormat.LINE_TOKENIZE_VALUE_DEFAULT);

  @Test
  public void testSmallConnectedGraph()
    throws Exception {
    String[] graph = getSmallConnectedGraph();
    validateSmallConnectedGraphResult(computeResults(graph));
  }

//  @Test
//  public void testCompleteBiPartiteGraph()
//    throws Exception {
//    String[] graph = getCompleteBiPartiteGraph();
//    validateCompleteBiPartiteGraphResult(computeResults(graph));
//  }

  /**
   * @return a small graph with two connected partitions
   */
  protected String[] getSmallConnectedGraph() {
    return new String[]{
      "0 0 0 1 2 3",
      "1 1 1 0 2 3",
      "2 2 0 0 1 3",
      "3 3 1 0 1 2"
    };
  }

  /**
   * @return a small complete bipartite graph
   */
  protected String[] getCompleteBiPartiteGraph() {
    return new String[]{
      "1 1 0 5 6 7 8",
      "2 2 0 5 6 7 8",
      "3 3 0 5 6 7 8",
      "4 4 0 5 6 7 8",
      "5 5 0 1 2 3 4",
      "6 6 0 1 2 3 4",
      "7 7 0 1 2 3 4",
      "8 8 0 1 2 3 4",
    };
  }

  private void validateSmallConnectedGraphResult(
    Map<Integer, Integer> vertexIDwithValue) {
    assertEquals(4, vertexIDwithValue.size());

    if (1 == vertexIDwithValue.get(0).intValue()) {
      if (0 == vertexIDwithValue.get(1).intValue()) {
        if (1 == vertexIDwithValue.get(2).intValue()) {
          if (0 == vertexIDwithValue.get(3).intValue()) {
            assertEquals(1, vertexIDwithValue.get(0).intValue());
            assertEquals(0, vertexIDwithValue.get(1).intValue());
            assertEquals(1, vertexIDwithValue.get(2).intValue());
            assertEquals(0, vertexIDwithValue.get(3).intValue());
          }
        }
      }
    }

    if (0 == vertexIDwithValue.get(0).intValue()) {
      if (1 == vertexIDwithValue.get(1).intValue()) {
        if (0 == vertexIDwithValue.get(2).intValue()) {
          if (1 == vertexIDwithValue.get(3).intValue()) {
            assertEquals(0, vertexIDwithValue.get(0).intValue());
            assertEquals(1, vertexIDwithValue.get(1).intValue());
            assertEquals(0, vertexIDwithValue.get(2).intValue());
            assertEquals(1, vertexIDwithValue.get(3).intValue());
          }
        }
      }
    }

    if (1 == vertexIDwithValue.get(0).intValue()) {
      if (1 == vertexIDwithValue.get(1).intValue()) {
        if (1 == vertexIDwithValue.get(2).intValue()) {
          if (1 == vertexIDwithValue.get(3).intValue()) {
            assertEquals(1, vertexIDwithValue.get(0).intValue());
            assertEquals(1, vertexIDwithValue.get(1).intValue());
            assertEquals(1, vertexIDwithValue.get(2).intValue());
            assertEquals(1, vertexIDwithValue.get(3).intValue());
          }
        }
      }
    }

    if (0 == vertexIDwithValue.get(0).intValue()) {
      if (0 == vertexIDwithValue.get(1).intValue()) {
        if (0 == vertexIDwithValue.get(2).intValue()) {
          if (0 == vertexIDwithValue.get(3).intValue()) {
            assertEquals(0, vertexIDwithValue.get(0).intValue());
            assertEquals(0, vertexIDwithValue.get(1).intValue());
            assertEquals(0, vertexIDwithValue.get(2).intValue());
            assertEquals(0, vertexIDwithValue.get(3).intValue());
          }
        }
      }
    }


  }

  private void validateCompleteBiPartiteGraphResult(Map<Integer,
    Integer> vertexIDwithValue) {
    assertEquals(8, vertexIDwithValue.size());
    if (0 == vertexIDwithValue.get(1)) {
      assertEquals(0, vertexIDwithValue.get(1).intValue());
      assertEquals(0, vertexIDwithValue.get(2).intValue());
      assertEquals(0, vertexIDwithValue.get(3).intValue());
      assertEquals(0, vertexIDwithValue.get(4).intValue());
      assertEquals(0, vertexIDwithValue.get(5).intValue());
      assertEquals(0, vertexIDwithValue.get(6).intValue());
      assertEquals(0, vertexIDwithValue.get(7).intValue());
      assertEquals(0, vertexIDwithValue.get(8).intValue());
    }

    if (1 == vertexIDwithValue.get(1)) {
      assertEquals(1, vertexIDwithValue.get(1).intValue());
      assertEquals(1, vertexIDwithValue.get(2).intValue());
      assertEquals(1, vertexIDwithValue.get(3).intValue());
      assertEquals(1, vertexIDwithValue.get(4).intValue());
      assertEquals(1, vertexIDwithValue.get(5).intValue());
      assertEquals(1, vertexIDwithValue.get(6).intValue());
      assertEquals(1, vertexIDwithValue.get(7).intValue());
      assertEquals(1, vertexIDwithValue.get(8).intValue());
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
