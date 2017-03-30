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
package org.gradoop.flink.model.impl.operators.nest2.operator;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.functions.tuple.Value1Of2;
import org.gradoop.flink.model.impl.operators.nest2.functions.CollectVertices;
import org.gradoop.flink.model.impl.operators.nest2.functions.AssociateAndMark;
import org.gradoop.flink.model.impl.operators.nest.functions.projections.Hex4;
import org.gradoop.flink.model.impl.operators.nest2.model.ops.BinaryOp;
import org.gradoop.flink.model.impl.operators.nest2.model.FlatModel;
import org.gradoop.flink.model.impl.operators.nest2.model.indices.IndexingForNesting;
import org.gradoop.flink.model.impl.operators.nest2.model.indices.NestedIndexing;
import org.gradoop.flink.model.impl.operators.nest.tuples.Hexaplet;
import org.gradoop.flink.model.impl.operators.nest2.functions.SetEdgesToNewGraph;

/**
 * Performs the vertex nesting operation. With this result the sole vertices are grouped.
 * Different edge semantics could be adopted later on. Those semantics must support the
 * IndexingForNesting format.
 */
public class Nesting extends BinaryOp<NestedIndexing,NestedIndexing,IndexingForNesting> {

  /**
   * GraphId to be associated to the graph that is going to be returned by this operator
   */
  private final GradoopId newGraphId;

  /**
   * the ExecutionEnvironment used to create the new GraphHead
   */
  private final ExecutionEnvironment ee;

  /**
   * Constructor for specifying the to-be-returned graph's head
   * @param newGraphId                  the aforementioned id
   * @param ee                          the ExecutionEnvironment used to create the new GraphHead
   */
  public Nesting(GradoopId newGraphId, ExecutionEnvironment ee) {
    this.newGraphId = newGraphId;
    this.ee = ee;
  }

  @Override
  protected IndexingForNesting runWithArgAndLake(FlatModel dataLake, NestedIndexing gU,
    NestedIndexing hypervertices) {

    DataSet<GradoopId> gh = ee.fromElements(newGraphId);
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
    DataSet<Tuple2<GradoopId, GradoopId>> vertices = hexas
      .groupBy(new Hex4())
      .reduceGroup(new CollectVertices(newGraphId));

    DataSet<Tuple2<GradoopId, GradoopId>> edges = gU.getGraphHeadToEdge()
      .map(new SetEdgesToNewGraph(newGraphId));

    return new IndexingForNesting(gh,vertices,edges,hexas);
  }

  @Override
  public String getName() {
    return getClass().getName();
  }

}
