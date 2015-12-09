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
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphElement;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.BinaryCollectionToValueOperator;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.functions.bool.And;
import org.gradoop.model.impl.functions.bool.Equals;
import org.gradoop.model.impl.functions.bool.Not;
import org.gradoop.model.impl.functions.bool.Or;
import org.gradoop.model.impl.operators.count.Count;
import org.gradoop.model.impl.functions.epgm.Tuple1WithId;
import org.gradoop.model.impl.functions.join.LeftSide;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;
import org.gradoop.model.impl.operators.equality.functions
  .GraphIdElementIdInTuple2;
import org.gradoop.model.impl.operators.equality.functions
  .GraphIdElementIdsInTuple2;
import org.gradoop.model.impl.operators.equality.functions
  .GraphIdVertexIdsEdgeIdsTriple;
import org.gradoop.model.impl.operators.equality.functions
  .VertexIdsEdgeIdsCountTriple;

/**
 * Two graph collections are equal,
 * if there exists an 1:1 mapping if graphs by vertex and edge id equality.
 *
 * @param <G> graph type
 * @param <V> vertex type
 * @param <E> edge type
 */
public class EqualityByGraphElementIds
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  extends EqualityBase<G, V, E>
  implements BinaryCollectionToValueOperator<G, V, E, Boolean> {

  @Override
  public DataSet<Boolean> execute(GraphCollection<G, V, E> firstCollection,
    GraphCollection<G, V, E> secondCollection) {

    DataSet<Tuple3<GradoopIdSet, GradoopIdSet, Long>> firstGraphsWithCount =
      getGraphElementIdsWithCount(firstCollection);

    DataSet<Tuple3<GradoopIdSet, GradoopIdSet, Long>> secondGraphsWithCount =
      getGraphElementIdsWithCount(secondCollection);

    DataSet<Long> distinctFirstGraphCount = Count
      .count(firstGraphsWithCount);

    DataSet<Long> matchingIdCount = Count.count(
      firstGraphsWithCount
        .join(secondGraphsWithCount)
        .where(0, 1, 2).equalTo(0, 1, 2)
    );

    DataSet<Boolean> firstCollectionIsEmpty = firstCollection.isEmpty();

    return Or.union(
      And.cross(firstCollectionIsEmpty, secondCollection.isEmpty()),
      And.cross(
        Not.map(firstCollectionIsEmpty),
        Equals.cross(distinctFirstGraphCount, matchingIdCount)
      )
    );
  }

  /**
   * Count occurrences of a combination of vertex and edge ids in a graph
   * collection.
   *
   * @param graphCollection input
   * @return count of vertex and edge id combinations
   */
  private DataSet<Tuple3<GradoopIdSet, GradoopIdSet, Long>>
  getGraphElementIdsWithCount(GraphCollection<G, V, E> graphCollection) {

    DataSet<Tuple2<GradoopId, Long>> firstGraphIdOccurrences =
      getIdsWithCount(graphCollection);

    DataSet<Tuple1<GradoopId>> graphIds = graphCollection.getGraphHeads()
      .map(new Tuple1WithId<G>());

    DataSet<Tuple2<GradoopId, GradoopIdSet>> vertexIdsByGraphId =
      getElementIdsByGraphId(
        graphIds, graphCollection.getVertices());

    DataSet<Tuple2<GradoopId, GradoopIdSet>> edgeIdsByGraphId =
      getElementIdsByGraphId(
        graphIds, graphCollection.getEdges());

    return vertexIdsByGraphId
      .join(edgeIdsByGraphId)
      .where(0).equalTo(0)
      .with(new GraphIdVertexIdsEdgeIdsTriple())
      .join(firstGraphIdOccurrences)
      .where(0).equalTo(0)
      .with(new VertexIdsEdgeIdsCountTriple())
      .groupBy(0, 1)
      .sum(2);
  }

  /**
   * Returns a pair of graph id and vertex or edge ids for each graph id in a
   * given data set.
   *
   * @param graphIds graph ids
   * @param elements vertices or edges
   * @param <GE> vertex or edge type
   * @return element ids per graph id pair
   */
  private <GE extends EPGMGraphElement> GroupReduceOperator
    <Tuple2<GradoopId, GradoopId>, Tuple2<GradoopId, GradoopIdSet>>
  getElementIdsByGraphId(
    DataSet<Tuple1<GradoopId>> graphIds, DataSet<GE> elements) {
    return elements
      .flatMap(new GraphIdElementIdInTuple2<GE>())
      .join(graphIds)
      .where(0).equalTo(0)
      .with(new LeftSide<Tuple2<GradoopId, GradoopId>,
            Tuple1<GradoopId>>())
      .groupBy(0)
      .reduceGroup(new GraphIdElementIdsInTuple2());
  }

  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }
}
