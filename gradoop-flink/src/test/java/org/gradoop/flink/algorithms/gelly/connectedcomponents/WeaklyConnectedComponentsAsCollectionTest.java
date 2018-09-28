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
package org.gradoop.flink.algorithms.gelly.connectedcomponents;

import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

public class WeaklyConnectedComponentsAsCollectionTest extends GradoopFlinkTestBase {

  private static final String propertyKey = "componentId";

  @Test
  public void testByElementIds() throws Exception {
    String graph = "input[" +
    // First component
    "(v0 {id:0, component:1})" +
    // Second component
    "(v1 {id:1, component:2})-[e0]->(v2 {id:2, component:2})" +
    "(v1)-[e1]->(v3 {id:3, component:2})" +
    "(v2)-[e2]->(v3)" +
    "(v3)-[e3]->(v4 {id:4, component:2})" +
    "(v4)-[e4]->(v5 {id:5, component:2})" +
    "]";

    FlinkAsciiGraphLoader loader = getLoaderFromString(graph);
    LogicalGraph input = loader.getLogicalGraphByVariable("input");
    GraphCollection result = input
      .callForCollection(new WeaklyConnectedComponentsAsCollection(propertyKey, 10));
    GraphCollection components = input.splitBy("component");

    collectAndAssertTrue(result.equalsByGraphElementIds(components));
  }
}