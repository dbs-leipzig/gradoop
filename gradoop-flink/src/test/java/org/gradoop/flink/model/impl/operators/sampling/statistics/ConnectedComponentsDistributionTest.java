/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.model.impl.operators.sampling.statistics;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test class for connected component distribution
 */
@RunWith(Parameterized.class)
public class ConnectedComponentsDistributionTest extends GradoopFlinkTestBase {

  /**
   * Name for test-case.
   */
  private String testName;

  /**
   * Property key to store the component id.
   */
  private final String propertyKey;

  /**
   * Maximum number of iterations.
   */
  private final int maxIterations;

  /**
   * Whether to write the component property to the edges.
   */
  private final boolean annotateEdges;

  /**
   * Test class constructor for parameterized call.
   *
   * @param testName Name for test-case
   * @param propertyKey Property key to store the component id
   * @param maxIterations Maximum number of iterations
   * @param annotateEdges Whether to write the component property to the edges
   */
  public ConnectedComponentsDistributionTest(String testName, String propertyKey, String maxIterations,
    String annotateEdges) {
    this.testName = testName;
    this.propertyKey = propertyKey;
    this.maxIterations = maxIterations.equals("MAX") ? Integer.MAX_VALUE : Integer.parseInt(maxIterations);
    this.annotateEdges = Boolean.parseBoolean(annotateEdges);
  }

  @Test
  public void testComponentDistribution() throws Exception {

    // Graph containing 3 WCC with: component - #vertices, #edges:
    // comp1 - 3, 3
    // comp2 - 4, 12
    // comp3 - 2, 1
    String graphString = "g[" +
      "/* first component */" +
      "(v0 {id:0, value:\"A\"})" +
      "(v1 {id:1, value:\"B\"})" +
      "(v2 {id:2, value:\"C\"})" +
      "(v0)-[e0]->(v1)" +
      "(v1)-[e1]->(v2)" +
      "(v2)-[e2]->(v0)" +
      "/* second component */" +
      "(v3 {id:3, value:\"D\"})" +
      "(v4 {id:4, value:\"E\"})" +
      "(v5 {id:5, value:\"F\"})" +
      "(v6 {id:6, value:\"G\"})" +
      "(v3)-[e4]->(v4)" +
      "(v3)-[e5]->(v5)" +
      "(v3)-[e6]->(v6)" +
      "(v4)-[e7]->(v3)" +
      "(v4)-[e8]->(v5)" +
      "(v4)-[e9]->(v6)" +
      "(v5)-[e10]->(v3)" +
      "(v5)-[e11]->(v4)" +
      "(v5)-[e12]->(v6)" +
      "(v6)-[e13]->(v3)" +
      "(v6)-[e14]->(v4)" +
      "(v6)-[e15]->(v5)" +
      "/* third component */" +
      "(v7 {id:7, value:\"H\"})" +
      "(v8 {id:8, value:\"I\"})" +
      "(v7)-[e3]->(v8)" +
      "]";

    LogicalGraph graph = getLoaderFromString(graphString).getLogicalGraphByVariable("g");
    DataSet<Tuple3<String, Long, Long>> componentDist = new ConnectedComponentsDistribution(
      propertyKey, maxIterations, annotateEdges).execute(graph);

    List<Tuple3<String, Long, Long>> componentDistList = Lists.newArrayList();

    componentDist.output(new LocalCollectionOutputFormat<>(componentDistList));

    getExecutionEnvironment().execute();

    assertEquals("Wrong number of components", 3, componentDistList.size());

    List<Long> vertexCount = componentDistList.stream()
      .map(tuple -> tuple.f1).collect(Collectors.toList());

    assertTrue("Wrong number of vertices per component",
      vertexCount.contains(4L) && vertexCount.contains(3L) && vertexCount.contains(2L));

    if(annotateEdges) {
      for(Tuple3<String, Long, Long> componentTuple : componentDistList) {
        if(componentTuple.f1 == 4L) {
          assertEquals("Wrong number of edges for component '" + componentTuple.f0 + "'",
            12L, (long) componentTuple.f2);
        }
        if(componentTuple.f1 == 3L) {
          assertEquals("Wrong number of edges for component '" + componentTuple.f0 + "'",
            3L, (long) componentTuple.f2);
        }
        if(componentTuple.f1 == 2L) {
          assertEquals("Wrong number of edges for component '" + componentTuple.f0 + "'",
            1L, (long) componentTuple.f2);
        }
      }
    }
  }

  /**
   * Parameters called when running the test
   *
   * @return List of parameters
   */
  @Parameterized.Parameters(name = "{index}: {0}")
  public static Iterable data() {
    return Arrays.asList(
      new String[] {
        "wcc with annotated edges and int.max iterations",
        "wcc_id",
        "MAX",
        "true"
      },
      new String[] {
        "wcc without annotated edges and 10 iterations",
        "wcc_id",
        "10",
        "false"
      });
  }
}