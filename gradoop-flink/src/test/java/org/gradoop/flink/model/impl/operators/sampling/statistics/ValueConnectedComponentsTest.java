/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Test for ValueConnectedComponents
 */
public class ValueConnectedComponentsTest extends GradoopFlinkTestBase {

  /**
   * Tests calculated component ids and checks if valid.
   *
   * @throws Exception in case of failure.
   */
  @Test
  public void testValueConnectedComponents() throws Exception {
    // Graph containing 3 WCC with: component - #vertices:
    // comp1 - 3
    // comp2 - 4
    // comp3 - 2
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

    // read graph
    FlinkAsciiGraphLoader loader = new FlinkAsciiGraphLoader(getConfig());
    loader.initDatabaseFromString(graphString);
    LogicalGraph graph = loader.getLogicalGraphByVariable("g");

    // execute Gelly ConnectedComponents.
    DataSet<Tuple2<Long, Long>> cComponents =
      new ValueConnectedComponentsDistribution(Integer.MAX_VALUE).execute(graph);

    List<Tuple2<Long, Long>> vertexComponentList = new ArrayList<>();
    cComponents.output(new LocalCollectionOutputFormat<>(vertexComponentList));

    // execute
    getExecutionEnvironment().execute();

    // get results map
    Map<Long, Long> componentsMap = getMapOfComponents(vertexComponentList);

    assertEquals("Wrong number of Vertices", 9, vertexComponentList.size());
    assertEquals("Wrong number of components", 3, componentsMap.size());
  }

  /**
   * Parses the results into a {@code Map<ComponentID,Count(v)>}.
   *
   * @param vertexComponentList lists of vertices with its corresponding component id
   * @return parsed map.
   */
  private Map<Long, Long> getMapOfComponents(List<Tuple2<Long, Long>> vertexComponentList) {

    Map<Long, Long> components = new HashMap<>();

    for (Tuple2<Long, Long> tuple : vertexComponentList) {
      if (!components.containsKey(tuple.f1)) {
        components.put(tuple.f1, 1L);
      } else {
        components.put(tuple.f1, components.get(tuple.f1) + 1L);
      }
    }
    return components;
  }
}
