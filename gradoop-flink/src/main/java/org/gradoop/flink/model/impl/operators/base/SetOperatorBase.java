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

package org.gradoop.flink.model.impl.operators.base;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.SourceId;
import org.gradoop.flink.model.impl.functions.epgm.TargetId;
import org.gradoop.flink.model.impl.functions.graphcontainment
  .PairVertexWithGraphs;
import org.gradoop.flink.model.impl.functions.utils.LeftSide;
import org.gradoop.flink.model.impl.operators.base.functions.LeftJoin0OfTuple2;
import org.gradoop.flink.model.impl.operators.difference.Difference;
import org.gradoop.flink.model.impl.operators.intersection.Intersection;
import org.gradoop.flink.model.impl.operators.union.Union;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Base class for set operations that share common methods to build vertex,
 * edge and data sets.
 *
 * @see Difference
 * @see Intersection
 * @see Union
 */
public abstract class SetOperatorBase extends
  BinaryCollectionToCollectionOperatorBase {

  /**
   * Computes new vertices based on the new subgraphs. For each vertex, each
   * graph is collected in a flatMap function and then joined with the new
   * subgraph dataset.
   *
   * @param newGraphHeads graph dataset of the resulting graph collection
   * @return vertex set of the resulting graph collection
   */
  @Override
  protected DataSet<Vertex> computeNewVertices(
    DataSet<GraphHead> newGraphHeads) {

    DataSet<Tuple2<Vertex, GradoopId>> verticesWithGraphs =
      firstCollection.getVertices().flatMap(new PairVertexWithGraphs<>());

    return verticesWithGraphs
      .join(newGraphHeads)
      .where(1)
      .equalTo(new Id<GraphHead>())
      .with(new LeftJoin0OfTuple2<Vertex, GraphHead>())
      .withForwardedFieldsFirst("f0->*")
      .distinct(new Id<Vertex>());
  }

  /**
   * Constructs new edges by joining the edges of the first graph with the new
   * vertices.
   *
   * @param newVertices vertex set of the resulting graph collection
   * @return edges set only connect vertices in {@code newVertices}
   * @see Difference
   * @see Intersection
   */
  @Override
  protected DataSet<Edge> computeNewEdges(DataSet<Vertex> newVertices) {
    return firstCollection.getEdges().join(newVertices)
      .where(new SourceId<>())
      .equalTo(new Id<Vertex>())
      .with(new LeftSide<Edge, Vertex>())
      .join(newVertices)
      .where(new TargetId<>())
      .equalTo(new Id<Vertex>())
      .with(new LeftSide<Edge, Vertex>())
      .distinct(new Id<Edge>());
  }
}
