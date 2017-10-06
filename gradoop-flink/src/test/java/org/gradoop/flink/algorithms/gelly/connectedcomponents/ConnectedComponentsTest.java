/**
 * Copyright © 2014 - 2017 Leipzig University (Database Research Group)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.algorithms.gelly.connectedcomponents;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.bool.And;
import org.gradoop.flink.model.impl.functions.utils.EqualsByPropertyValue;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

public class ConnectedComponentsTest extends GradoopFlinkTestBase {

  private static final String propertyKey = "componentId";

  @Test
  public void testByElementIds() throws Exception {
    String graph = "input[" +
    // First component
    "(v0 {id:0})" +
    // Second component
    "(v1 {id:1})-[e0]->(v2 {id:2})" +
    "(v1)-[e1]->(v3 {id:3})" +
    "(v2)-[e2]->(v3)" +
    "(v3)-[e3]->(v4 {id:4})" +
    "(v4)-[e5]->(v5 {id:5})" +
    "]" +
    // Result:
    // First component
    "result0[" +
    "(v0)" +
    "]" +
    // Second component
    "result1[" +
    "(v1)" +
    "(v2)" +
    "(v3)" +
    "(v4)" +
    "(v5)" +
    "]";

    FlinkAsciiGraphLoader loader = getLoaderFromString(graph);
    LogicalGraph input = loader.getLogicalGraphByVariable("input");
    LogicalGraph component0 = loader.getLogicalGraphByVariable("result0");
    LogicalGraph component1 = loader.getLogicalGraphByVariable("result1");
    LogicalGraph result = input.callForGraph(new ConnectedComponents(propertyKey, 10));


    DataSet<Boolean> isOverlapWith0 = result.overlap(component0).getVertices()
      .map(new EqualsByPropertyValue<>(propertyKey)).reduce(new And());
    collectAndAssertTrue(isOverlapWith0);

    DataSet<Boolean> isOverlapWith1 = result.overlap(component1).getVertices()
      .map(new EqualsByPropertyValue<>(propertyKey)).reduce(new And());
    collectAndAssertTrue(isOverlapWith1);
  }

}