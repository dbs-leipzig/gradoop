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
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.BinaryCollectionToValueOperator;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.functions.bool.And;
import org.gradoop.model.impl.functions.bool.Equals;
import org.gradoop.model.impl.functions.bool.Not;
import org.gradoop.model.impl.functions.bool.Or;
import org.gradoop.model.impl.operators.count.Count;
import org.gradoop.model.impl.id.GradoopId;

/**
 * Two graph collections are equal,
 * if they contain the same graphs by id.
 *
 * @param <G> graph type
 * @param <V> vertex type
 * @param <E> edge type
 */
public class EqualityByGraphIds
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  extends EqualityBase<G, V, E>
  implements BinaryCollectionToValueOperator<G, V, E, Boolean> {

  @Override
  public DataSet<Boolean> execute(
    GraphCollection<G, V, E> firstCollection,
    GraphCollection<G, V, E> secondCollection) {

    DataSet<Tuple2<GradoopId, Long>> firstGraphIdsWithCount =
      getIdsWithCount(firstCollection);

    DataSet<Tuple2<GradoopId, Long>> secondGraphIdsWithCount =
      getIdsWithCount(secondCollection);

    DataSet<Long> distinctFirstIdCount = Count
      .count(firstGraphIdsWithCount);

    DataSet<Long> matchingIdCount = Count.count(
      firstGraphIdsWithCount
        .join(secondGraphIdsWithCount)
        .where(0, 1).equalTo(0, 1)
    );

    DataSet<Boolean> firstCollectionIsEmpty = firstCollection.isEmpty();

    return Or.union(
      And.cross(firstCollectionIsEmpty, secondCollection.isEmpty()),
      And.cross(
        Not.map(firstCollectionIsEmpty),
        Equals.cross(distinctFirstIdCount, matchingIdCount)
      )

    );
  }

  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }
}
