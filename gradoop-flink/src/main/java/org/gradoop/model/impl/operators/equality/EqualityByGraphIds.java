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

    return Or.union(
      And.cross(firstCollection.isEmpty(), secondCollection.isEmpty()),
      Equals.cross(distinctFirstIdCount, matchingIdCount)
    );
  }

  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }
}
