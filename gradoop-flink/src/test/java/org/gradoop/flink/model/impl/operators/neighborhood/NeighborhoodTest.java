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
package org.gradoop.flink.model.impl.operators.neighborhood;

import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.aggregation.functions.sum.SumEdgeProperty;
import org.gradoop.flink.model.impl.operators.aggregation.functions.sum.SumVertexProperty;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

public class NeighborhoodTest extends GradoopFlinkTestBase {

  // Reduce on Edge Tests

  @Test
  public void testReduceOnEdgesSumAggIncEdge() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("input[" +
      "(v0:Blue {a : 3})" +
      "(v1:Blue {a : 2})" +
      "(v2:Blue {a : 4})" +
      "(v0)-[{b : 2}]->(v1)" +
      "(v0)-[{b : 1}]->(v2)" +
      "(v1)-[{b : 3}]->(v2)" +
      "(v2)-[{b : 4}]->(v0)" +
      "]");

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Blue {a : 3,sum_b : 4})" +
      "(v01:Blue {a : 2,sum_b : 2})" +
      "(v02:Blue {a : 4,sum_b : 4})" +
      "(v00)-[{b : 2}]->(v01)" +
      "(v00)-[{b : 1}]->(v02)" +
      "(v01)-[{b : 3}]->(v02)" +
      "(v02)-[{b : 4}]->(v00)" +
      "]");

    LogicalGraph output = input
      .reduceOnEdges(new SumEdgeProperty("b"), Neighborhood.EdgeDirection.IN);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testReduceOnEdgesSumAggOutEdge() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("input[" +
      "(v0:Blue {a : 3})" +
      "(v1:Blue {a : 2})" +
      "(v2:Blue {a : 4})" +
      "(v0)-[{b : 2}]->(v1)" +
      "(v0)-[{b : 1}]->(v2)" +
      "(v1)-[{b : 3}]->(v2)" +
      "(v2)-[{b : 4}]->(v0)" +
      "]");

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Blue {a : 3,sum_b : 3})" +
      "(v01:Blue {a : 2,sum_b : 3})" +
      "(v02:Blue {a : 4,sum_b : 4})" +
      "(v00)-[{b : 2}]->(v01)" +
      "(v00)-[{b : 1}]->(v02)" +
      "(v01)-[{b : 3}]->(v02)" +
      "(v02)-[{b : 4}]->(v00)" +
      "]");

    LogicalGraph output = input
      .reduceOnEdges(new SumEdgeProperty("b"), Neighborhood.EdgeDirection.OUT);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testReduceOnEdgesSumAggBothEdges() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("input[" +
      "(v0:Blue {a : 3})" +
      "(v1:Blue {a : 2})" +
      "(v2:Blue {a : 4})" +
      "(v0)-[{b : 2}]->(v1)" +
      "(v0)-[{b : 1}]->(v2)" +
      "(v1)-[{b : 3}]->(v2)" +
      "(v2)-[{b : 4}]->(v0)" +
      "]");

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Blue {a : 3,sum_b : 7})" +
      "(v01:Blue {a : 2,sum_b : 5})" +
      "(v02:Blue {a : 4,sum_b : 8})" +
      "(v00)-[{b : 2}]->(v01)" +
      "(v00)-[{b : 1}]->(v02)" +
      "(v01)-[{b : 3}]->(v02)" +
      "(v02)-[{b : 4}]->(v00)" +
      "]");

    LogicalGraph output = input
      .reduceOnEdges(new SumEdgeProperty("b"), Neighborhood.EdgeDirection.BOTH);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  // Reduce on Neighbor Tests

  @Test
  public void testReduceOnNeighborsSumAggIncEdge() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("input[" +
      "(v0:Blue {a : 3})" +
      "(v1:Blue {a : 2})" +
      "(v2:Blue {a : 4})" +
      "(v0)-[{b : 2}]->(v1)" +
      "(v0)-[{b : 1}]->(v2)" +
      "(v1)-[{b : 3}]->(v2)" +
      "(v2)-[{b : 4}]->(v0)" +
      "]");

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Blue {a : 3,sum_a : 4})" +
      "(v01:Blue {a : 2,sum_a : 3})" +
      "(v02:Blue {a : 4,sum_a : 5})" +
      "(v00)-[{b : 2}]->(v01)" +
      "(v00)-[{b : 1}]->(v02)" +
      "(v01)-[{b : 3}]->(v02)" +
      "(v02)-[{b : 4}]->(v00)" +
      "]");

    LogicalGraph output = input
      .reduceOnNeighbors(new SumVertexProperty("a"), Neighborhood.EdgeDirection.IN);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testReduceOnNeighborsSumAggOutEdge() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("input[" +
      "(v0:Blue {a : 3})" +
      "(v1:Blue {a : 2})" +
      "(v2:Blue {a : 4})" +
      "(v0)-[{b : 2}]->(v1)" +
      "(v0)-[{b : 1}]->(v2)" +
      "(v1)-[{b : 3}]->(v2)" +
      "(v2)-[{b : 4}]->(v0)" +
      "]");

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Blue {a : 3,sum_a : 6})" +
      "(v01:Blue {a : 2,sum_a : 4})" +
      "(v02:Blue {a : 4,sum_a : 3})" +
      "(v00)-[{b : 2}]->(v01)" +
      "(v00)-[{b : 1}]->(v02)" +
      "(v01)-[{b : 3}]->(v02)" +
      "(v02)-[{b : 4}]->(v00)" +
      "]");

    LogicalGraph output = input
      .reduceOnNeighbors(new SumVertexProperty("a"), Neighborhood.EdgeDirection.OUT);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testReduceOnNeighborsSumAggBothEdges() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("input[" +
      "(v0:Blue {a : 3})" +
      "(v1:Blue {a : 2})" +
      "(v2:Blue {a : 4})" +
      "(v0)-[{b : 2}]->(v1)" +
      "(v0)-[{b : 1}]->(v2)" +
      "(v1)-[{b : 3}]->(v2)" +
      "(v2)-[{b : 4}]->(v0)" +
      "]");

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Blue {a : 3,sum_a : 10})" +
      "(v01:Blue {a : 2,sum_a : 7})" +
      "(v02:Blue {a : 4,sum_a : 8})" +
      "(v00)-[{b : 2}]->(v01)" +
      "(v00)-[{b : 1}]->(v02)" +
      "(v01)-[{b : 3}]->(v02)" +
      "(v02)-[{b : 4}]->(v00)" +
      "]");

    LogicalGraph output = input
      .reduceOnNeighbors(new SumVertexProperty("a"), Neighborhood.EdgeDirection.BOTH);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }
}
