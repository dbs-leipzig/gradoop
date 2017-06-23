/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.model.impl.operators.neighborhood;

import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.LogicalGraph;
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
