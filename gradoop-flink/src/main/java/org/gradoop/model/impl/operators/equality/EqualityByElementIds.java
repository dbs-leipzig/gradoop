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

package org.gradoop.model.impl.operators.equality;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.BinaryGraphToValueOperator;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.functions.bool.Or;
import org.gradoop.model.impl.operators.count.Count;
import org.gradoop.model.impl.functions.epgm.ToGradoopIdSet;
import org.gradoop.model.impl.functions.bool.And;
import org.gradoop.model.impl.functions.bool.Equals;
import org.gradoop.model.impl.functions.epgm.Id;
import org.gradoop.model.impl.id.GradoopIdSet;

/**
 * Tow graphs are equal,
 * if vertex and edge sets contain the same elements by id.
 *
 * @param <G> graph head type
 * @param <V> vertex type
 * @param <E> edge type
 */
public class EqualityByElementIds
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  extends EqualityBase implements BinaryGraphToValueOperator<V, E, G, Boolean> {

  @Override
  public DataSet<Boolean> execute(LogicalGraph<G, V, E> firstGraph,
    LogicalGraph<G, V, E> secondGraph) {

    DataSet<GradoopIdSet> firstGraphVertexIds = firstGraph.getVertices()
      .map(new Id<V>())
      .reduceGroup(new ToGradoopIdSet());

    DataSet<GradoopIdSet> secondGraphVertexIds = secondGraph.getVertices()
      .map(new Id<V>())
      .reduceGroup(new ToGradoopIdSet());

    DataSet<GradoopIdSet> firstGraphEdgeIds = firstGraph.getEdges()
      .map(new Id<E>())
      .reduceGroup(new ToGradoopIdSet());

    DataSet<GradoopIdSet> secondGraphEdgeIds = secondGraph.getEdges()
      .map(new Id<E>())
      .reduceGroup(new ToGradoopIdSet());

    return Or.union(
      And.cross(
        firstGraph.isEmpty(),
        secondGraph.isEmpty()),
      And.cross(
        Equals.cross(
          firstGraphVertexIds,
          secondGraphVertexIds),
        Or.union(
          And.cross(
            Count.isEmpty(firstGraphEdgeIds),
            Count.isEmpty(secondGraphEdgeIds)),
          Equals.cross(
            firstGraphEdgeIds,
            secondGraphEdgeIds)
        )
      )
    );
  }

  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }
}
