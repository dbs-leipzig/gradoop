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

import org.gradoop.dataintegration.transformation.impl.Neighborhood;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

/**
 * Tests for the {@link ConnectNeighbors} operator.
 */
public class ConnectNeighborsTest extends GradoopFlinkTestBase {

  /**
   * The loader used to get the test graphs.
   */
  private FlinkAsciiGraphLoader loader = getLoaderFromString("input:test [" +
    "(i:V)-->(c:Center)-->(o:V)" +
    "(i2:V)-->(c)-->(o2:V)" +
    "(:other)-->(c)-->(:other)" +
    "]" +
    "expectedIncoming:test [" +
    "(i)-[:neighbor]->(i2)" +
    "(i2)-[:neighbor]->(i)" +
    "(i:V)-->(c:Center)-->(o:V)" +
    "(i2:V)-->(c)-->(o2:V)" +
    "(:other)-->(c)-->(:other)" +
    "]" +
    "expectedOutgoing:test [" +
    "(o)-[:neighbor]->(o2)" +
    "(o2)-[:neighbor]->(o)" +
    "(i:V)-->(c:Center)-->(o:V)" +
    "(i2:V)-->(c)-->(o2:V)" +
    "(:other)-->(c)-->(:other)" +
    "]" +
    "expectedUndirected:test [" +
    "(i)-[:neighbor]->(i2)" +
    "(i2)-[:neighbor]->(i)" +
    "(o)-[:neighbor]->(o2)" +
    "(o2)-[:neighbor]->(o)" +
    "(i:V)-->(c:Center)-->(o:V)" +
    "(i2:V)-->(c)-->(o2:V)" +
    "(:other)-->(c)-->(:other)" +
    "]");

  /**
   * Test using incoming edges.
   *
   * @throws Exception when the execution in Flink fails.
   */
  @Test
  public void testIncoming() throws Exception {
    LogicalGraph input = loader.getLogicalGraphByVariable("input");
    UnaryGraphToGraphOperator operator =
      new ConnectNeighbors("Center", Neighborhood.EdgeDirection.INCOMING, "V", "neighbor");
    LogicalGraph expected = loader.getLogicalGraphByVariable("expectedIncoming");

    collectAndAssertTrue(expected.equalsByData(input.callForGraph(operator)));
  }

  /**
   * Test using outgoing edges.
   *
   * @throws Exception when the execution in Flink fails.
   */
  @Test
  public void testOutgoing() throws Exception {
    LogicalGraph input = loader.getLogicalGraphByVariable("input");
    UnaryGraphToGraphOperator operator =
      new ConnectNeighbors("Center", Neighborhood.EdgeDirection.OUTGOING, "V", "neighbor");
    LogicalGraph expected = loader.getLogicalGraphByVariable("expectedOutgoing");

    collectAndAssertTrue(expected.equalsByData(input.callForGraph(operator)));
  }

  /**
   * Test using edges in both directions.
   *
   * @throws Exception when the execution in Flink fails.
   */
  @Test
  public void testUndirected() throws Exception {
    LogicalGraph input = loader.getLogicalGraphByVariable("input");
    UnaryGraphToGraphOperator operator =
      new ConnectNeighbors("Center", Neighborhood.EdgeDirection.UNDIRECTED, "V", "neighbor");
    LogicalGraph expected = loader.getLogicalGraphByVariable("expectedUndirected");

    collectAndAssertTrue(expected.equalsByData(input.callForGraph(operator)));
  }
}
