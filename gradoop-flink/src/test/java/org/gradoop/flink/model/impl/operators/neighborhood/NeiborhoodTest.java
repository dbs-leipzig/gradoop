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

public class NeiborhoodTest extends GradoopFlinkTestBase {


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
      "(v0:Blue {a : 3,sum_b : 4})" +
      "(v1:Blue {a : 2,sum_b : 2})" +
      "(v2:Blue {a : 4,sum_b : 4})" +
      "(v0)-[{b : 2}]->(v1)" +
      "(v0)-[{b : 1}]->(v2)" +
      "(v1)-[{b : 3}]->(v2)" +
      "(v2)-[{b : 4}]->(v0)" +
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
      "(v0:Blue {a : 3,sum_b : 3})" +
      "(v1:Blue {a : 2,sum_b : 3})" +
      "(v2:Blue {a : 4,sum_b : 4})" +
      "(v0)-[{b : 2}]->(v1)" +
      "(v0)-[{b : 1}]->(v2)" +
      "(v1)-[{b : 3}]->(v2)" +
      "(v2)-[{b : 4}]->(v0)" +
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
      "(v0:Blue {a : 3,sum_b : 7})" +
      "(v1:Blue {a : 2,sum_b : 5})" +
      "(v2:Blue {a : 4,sum_b : 8})" +
      "(v0)-[{b : 2}]->(v1)" +
      "(v0)-[{b : 1}]->(v2)" +
      "(v1)-[{b : 3}]->(v2)" +
      "(v2)-[{b : 4}]->(v0)" +
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
      "(v0:Blue {a : 3,suma : 4})" +
      "(v1:Blue {a : 2,suma : 3})" +
      "(v2:Blue {a : 4,suma : 5})" +
      "(v0)-[{b : 2}]->(v1)" +
      "(v0)-[{b : 1}]->(v2)" +
      "(v1)-[{b : 3}]->(v2)" +
      "(v2)-[{b : 4}]->(v0)" +
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
      "(v0:Blue {a : 3,suma : 6})" +
      "(v1:Blue {a : 2,suma : 4})" +
      "(v2:Blue {a : 4,suma : 3})" +
      "(v0)-[{b : 2}]->(v1)" +
      "(v0)-[{b : 1}]->(v2)" +
      "(v1)-[{b : 3}]->(v2)" +
      "(v2)-[{b : 4}]->(v0)" +
      "]");

    LogicalGraph output = input
      .reduceOnNeighbors(new SumVertexProperty("a"), Neighborhood.EdgeDirection.OUT);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testReduceOnNeighborsSumAggBothEdges() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("input[" +
      "(v0:Blue {a : 3,b : 5})" +
      "(v1:Blue {a : 2})" +
      "(v2:Blue {a : 4})" +
      "(v0)-[{b : 2}]->(v1)" +
      "(v0)-[{b : 1}]->(v2)" +
      "(v1)-[{b : 3}]->(v2)" +
      "(v2)-[{b : 4}]->(v0)" +
      "]");

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v0:Blue {a : 3,sum_a : 10})" +
      "(v1:Blue {a : 2,sum_a : 7})" +
      "(v2:Blue {a : 4,sum_a : 8})" +
      "(v0)-[{b : 2}]->(v1)" +
      "(v0)-[{b : 1}]->(v2)" +
      "(v1)-[{b : 3}]->(v2)" +
      "(v2)-[{b : 4}]->(v0)" +
      "]");

    LogicalGraph output = input
      .reduceOnNeighbors(new SumVertexProperty("a"), Neighborhood.EdgeDirection.BOTH);

    System.out.println();
    System.out.println();

    loader.getLogicalGraphByVariable("expected").getVertices().print();

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }
}
