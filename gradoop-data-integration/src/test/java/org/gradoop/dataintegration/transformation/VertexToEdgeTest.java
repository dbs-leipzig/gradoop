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
package org.gradoop.dataintegration.transformation;

import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

/**
 * This class contains tests for the {@link VertexToEdge} transformation operator.
 */
public class VertexToEdgeTest extends GradoopFlinkTestBase {

  /**
   * Test the {@link VertexToEdge} transformation where one one edge is added.
   *
   * @throws Exception when the execution in Flink fails.
   */
  @Test
  public void testWithEdgeCreation() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("input:test[" +
      "(v0:Blue {a : 3})" +
      "(v1:Green {a : 2})" +
      "(v2:Blue {a : 4})" +
      "(v0)-[{b : 2}]->(v1)" +
      "(v1)-[{b : 4}]->(v2)" +
      "]" +
      "expected:test[" +
      "(v00:Blue {a : 3})" +
      "(v01:Green {a : 2})" +
      "(v02:Blue {a : 4})" +
      "(v00)-[{b : 2}]->(v01)" +
      "(v01)-[{b : 4}]->(v02)" +
      "(v00)-[:foo {a : 2, originalVertexLabel: \"Green\"," +
      "firstEdgeLabel: \"\", secondEdgeLabel: \"\"}]->(v02)" +
      "]");
    LogicalGraph input = loader.getLogicalGraphByVariable("input");
    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");

    VertexToEdge transformation = new VertexToEdge("Green", "foo");
    LogicalGraph transformed = input.callForGraph(transformation);

    collectAndAssertTrue(transformed.equalsByData(expected));
  }
}
