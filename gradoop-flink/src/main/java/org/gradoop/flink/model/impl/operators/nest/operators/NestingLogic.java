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
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */
package org.gradoop.flink.model.impl.operators.nest.operators;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.tuple.Value1Of2;
import org.gradoop.flink.model.impl.operators.nest.functions.AssociateAndMark;
import org.gradoop.flink.model.impl.operators.nest.functions.CollectVertices;
import org.gradoop.flink.model.impl.operators.nest.functions.Hex4;
import org.gradoop.flink.model.impl.operators.nest.model.indices.NestedResult;
import org.gradoop.flink.model.impl.operators.nest.model.indices.NestingIndex;
import org.gradoop.flink.model.impl.operators.nest.model.ops.BinaryOp;
import org.gradoop.flink.model.impl.operators.nest.tuples.Hexaplet;

/**
 * Performs the vertex nesting operation. With this result the sole vertices are grouped.
 * Different edge semantics could be adopted later on. Those semantics must support the
 * IndexingForNesting format.
 */
public class NestingLogic extends BinaryOp<NestingIndex, NestingIndex, NestedResult> {

  @Override
  protected NestedResult runWithArgAndLake(LogicalGraph dataLake, NestingIndex gU,
    NestingIndex hypervertices) {

    DataSet<GradoopId> gh = gU.getGraphHeads();
    // Associate each gid in hypervertices.H to the merged vertices
    //DataSet<GradoopId> subgraphIds = hypervertices.getGraphHeads();

    // Mark each vertex if either it's present or not in the final match
    // TODO       JOIN COUNT: (1)
    DataSet<Hexaplet> hexas = gU.getGraphHeadToVertex()
      .leftOuterJoin(hypervertices.getGraphHeadToVertex())
      .where(new Value1Of2<>()).equalTo(new Value1Of2<>())
      // If the vertex does not appear in the graph collection, the f2 element is null.
      // These vertices are the ones to be returned as vertices alongside with the new
      // graph heads
      .with(new AssociateAndMark());

    // Vertices to be returend within the NestedIndexing
    DataSet<GradoopId> tmpVert = hexas
      .groupBy(new Hex4())
      .reduceGroup(new CollectVertices());
    DataSet<Tuple2<GradoopId, GradoopId>> vertices = gh.crossWithHuge(tmpVert);

    DataSet<GradoopId> tmpEdges = gU
      .getGraphHeadToEdge()
      .map(new Value1Of2<>());
    DataSet<Tuple2<GradoopId, GradoopId>> edges = gh
      .crossWithHuge(tmpEdges);

    return new NestedResult(gh, vertices, edges, hexas);
  }

  @Override
  public String getName() {
    return getClass().getName();
  }

}
