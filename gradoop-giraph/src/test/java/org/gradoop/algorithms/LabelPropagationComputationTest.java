/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.algorithms;

import com.google.common.collect.Maps;
import junit.framework.TestCase;
import org.apache.giraph.conf.GiraphConfiguration;
import org.gradoop.algorithms.LabelPropagationComputation;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
import org.apache.giraph.io.formats.IntIntNullTextVertexInputFormat;
import org.apache.giraph.utils.InternalVertexRunner;
import org.junit.Test;

import java.util.Map;
import java.util.regex.Pattern;

/**
 * Tests for {@link org.gradoop.algorithms}
 */
public class LabelPropagationComputationTest extends TestCase {
  private static final Pattern LINE_TOKEN_SEPARATOR = Pattern.compile
    (IdWithValueTextOutputFormat.LINE_TOKENIZE_VALUE_DEFAULT);

  @Test
  public void testConnectedGraph()
    throws Exception {
    String[] graph = getConnectedGraph();
    validateConnectedGraphResult(computeResults(graph));
  }

  @Test
  public void testLoopGraph()
    throws Exception {
    String[] graph = getLoopGraph();
    validateLoopGraphResult(computeResults(graph));
  }




  @Test
  public void testDisconnectedGraph()
    throws Exception {
    String[] graph = getDisconnectedGraph();
    validateDisconnectedGraphResult(computeResults(graph));
  }

  @Test
  public void testBiPartitGraph()
    throws Exception {
    String[] graph = getBiPartiteGraph();
    validateBipartitGraphResult(computeResults(graph));
  }

  @Test
  public void testCompleteBiPartiteGraph()
    throws Exception {
    String[] graph = getCompleteBiPartiteGraph();
    validateCompleteBipartiteGraphResult(computeResults(graph));
  }


  /**
   * @return a small graph with two connected partitions
   */
  protected String[] getConnectedGraph() {
    return new String[]{
      "0 0 1 2 3",
      "1 1 0 2 3",
      "2 2 0 1 3 4",
      "3 3 0 1 2",
      "4 4 2 5 6 7",
      "5 5 4 6 7 8",
      "6 6 4 5 7",
      "7 7 4 5 6",
      "8 8 5 9 10 11",
      "9 9 8 10 11",
      "10 10 8 9 11",
      "11 11 8 9 10"
    };
  }


  /**
   * @return a small graph with two connected partitions
   */
  protected String[] getLoopGraph() {
    return new String[]{
      "0 0 0 0 0 0 0 0",
      "0 0 0"
    };
  }



  /**
   * @return a small graph with two disconnected partitions
   */
  protected String[] getDisconnectedGraph() {
    return new String[]{
      "0 0 1 2 3",
      "1 1 0 2 3",
      "2 2 0 1 3",
      "3 3 0 1 2",
      "4 4 5 6 7",
      "5 5 4 6 7",
      "6 6 4 5 7",
      "7 7 4 5 6"
    };
  }

  /**
   * @return a small bipartite graph
   */
  protected String[] getBiPartiteGraph() {
    return new String[]{
      "0 0 4",
      "1 1 5",
      "2 2 6",
      "3 3 7",
      "4 4 0",
      "5 5 1",
      "6 6 2",
      "7 7 3"
    };
  }

  /**
   * @return a small complete bipartite graph
   */
  protected String[] getCompleteBiPartiteGraph() {
    return new String[]{
      "1 1 5 6 7 8",
      "2 2 5 6 7 8",
      "3 3 5 6 7 8",
      "4 4 5 6 7 8",
      "5 5 1 2 3 4",
      "6 6 1 2 3 4",
      "7 7 1 2 3 4",
      "8 8 1 2 3 4",
    };
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

  private void validateBipartitGraphResult(
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
    assertEquals(1, vertexIDwithValue.get(1).intValue());
    assertEquals(1, vertexIDwithValue.get(2).intValue());
    assertEquals(1, vertexIDwithValue.get(3).intValue());
    assertEquals(1, vertexIDwithValue.get(4).intValue());
    assertEquals(1, vertexIDwithValue.get(5).intValue());
    assertEquals(1, vertexIDwithValue.get(6).intValue());
    assertEquals(1, vertexIDwithValue.get(7).intValue());
    assertEquals(1, vertexIDwithValue.get(8).intValue());
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
    conf.setVertexInputFormatClass(IntIntNullTextVertexInputFormat.class);
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

